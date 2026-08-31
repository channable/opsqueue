use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};

use crate::{
    common::{
        errors::{DatabaseError, E},
        submission::{self, SubmissionId, SubmissionStatus},
    },
    db::{Connection, DBPools},
};

pub(crate) type SubmissionStatusChangedSender = mpsc::UnboundedSender<SubmissionId>;

/// The narrow interface exposed by the Opsqueue core to optional integrations.
#[derive(Clone, Debug)]
pub struct Interface {
    pub(crate) pool: DBPools,
    submission_status_changed: Arc<Mutex<mpsc::UnboundedReceiver<SubmissionId>>>,
    status_changed_sender: SubmissionStatusChangedSender,
}

impl Interface {
    pub(crate) fn new(
        pool: DBPools,
        status_changed_sender: SubmissionStatusChangedSender,
        status_changed_receiver: mpsc::UnboundedReceiver<SubmissionId>,
    ) -> Self {
        Self {
            pool,
            submission_status_changed: Arc::new(Mutex::new(status_changed_receiver)),
            status_changed_sender,
        }
    }

    /// Unpauses all given submissions in one transaction.
    ///
    /// Submissions that are no longer paused are ignored. This makes the operation safe to retry
    /// after a delegation request has been delivered more than once.
    ///
    /// # Errors
    ///
    /// Returns an error if acquiring a writer connection or updating the database fails.
    pub async fn unpause_submissions(&self, ids: Vec<SubmissionId>) -> Result<(), DatabaseError> {
        let mut conn = self.pool.writer_conn().await?;
        let changed_ids = conn
            .transaction(move |mut tx| {
                Box::pin(async move {
                    let mut changed_ids = Vec::new();
                    for id in ids {
                        let was_paused =
                            match submission::db::unpause_submission_raw(id, &mut tx).await {
                                Ok(()) => {
                                    changed_ids.push(id);
                                    true
                                }
                                Err(E::R(_)) => false,
                                Err(E::L(error)) => return Err(error),
                            };
                        if was_paused {
                            crate::common::chunk::db::restore_paused_chunks(id, &mut tx).await?;
                            match submission::db::maybe_complete_submission(id, &mut tx).await {
                                Ok(_) | Err(E::R(_)) => {}
                                Err(E::L(error)) => return Err(error),
                            }
                        }
                    }
                    Ok(changed_ids)
                })
            })
            .await?;

        self.notify_status_changed(changed_ids);
        Ok(())
    }

    /// Cancels all given submissions in one transaction.
    ///
    /// Submissions that are already terminal or missing are ignored. This makes the operation safe
    /// to retry after a delegation request has been delivered more than once.
    ///
    /// # Errors
    ///
    /// Returns an error if acquiring a writer connection or updating the database fails.
    pub async fn cancel_submissions(&self, ids: Vec<SubmissionId>) -> Result<(), DatabaseError> {
        let mut conn = self.pool.writer_conn().await?;
        let changed_ids = conn
            .transaction(move |mut tx| {
                Box::pin(async move {
                    let mut changed_ids = Vec::new();
                    for id in ids {
                        match submission::db::cancel_submission_notx(id, &mut tx).await {
                            Ok(()) => changed_ids.push(id),
                            Err(E::R(_)) => {}
                            Err(E::L(error)) => return Err(error),
                        }
                    }
                    Ok(changed_ids)
                })
            })
            .await?;

        self.notify_status_changed(changed_ids);
        Ok(())
    }

    /// Gets the current status for each given submission ID.
    ///
    /// The returned vector has the same order as `ids`; a `None` entry means that the submission
    /// no longer exists.
    ///
    /// # Errors
    ///
    /// Returns an error if acquiring a reader connection or querying the database fails.
    pub async fn get_submission_statuses(
        &self,
        ids: Vec<SubmissionId>,
    ) -> Result<Vec<Option<SubmissionStatus>>, DatabaseError> {
        let mut conn = self.pool.reader_conn().await?;
        let mut statuses = Vec::with_capacity(ids.len());
        for id in ids {
            statuses.push(submission::db::submission_status(id, &mut conn).await?);
        }
        Ok(statuses)
    }

    /// Waits until any submission status changes.
    ///
    /// Returns `None` if the underlying channel is closed, which happens during shutdown.
    pub async fn wait_for_submission_status_change(&self) -> Option<SubmissionId> {
        self.submission_status_changed.lock().await.recv().await
    }

    fn notify_status_changed(&self, ids: Vec<SubmissionId>) {
        for id in ids {
            let _ = self.status_changed_sender.send(id);
        }
    }
}
