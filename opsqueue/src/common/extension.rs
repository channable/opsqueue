use crate::E;
use crate::common::errors::{DatabaseError, E, SubmissionNotCancellable, SubmissionNotFound};
use crate::common::submission::db::{cancel_submission, submission_status, unpause_submission};
use crate::common::submission::{SubmissionId, SubmissionStatus};
use crate::db;
use crate::db::{Connection, DBPools, WriterConnection};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Notify;

#[async_trait]
pub trait Extension: Send + Sync {
    /// Returns whether the extension references the given submission.
    /// Used to e.g. check if old submissions can be deleted safely.
    async fn references_submission<'t, 'conn>(
        &self,
        submission: SubmissionId,
        conn: &db::conn::Writer<db::conn::NoTransaction>,
    ) -> sqlx::Result<bool>;
}

#[derive(Debug)]
pub struct CoreApi {
    pub pool: DBPools,
    notify_on_insert: Arc<Notify>,
    submission_status_changed_tx: tokio::sync::broadcast::Sender<SubmissionId>,
    pub submission_status_changed_rx: tokio::sync::broadcast::Receiver<SubmissionId>,
}

impl CoreApi {
    pub(crate) fn new(
        pool: DBPools,
        notify_on_insert: Arc<Notify>,
        submission_status_changed_tx: tokio::sync::broadcast::Sender<SubmissionId>,
        submission_status_changed_rx: tokio::sync::broadcast::Receiver<SubmissionId>,
    ) -> Self {
        Self {
            pool,
            notify_on_insert,
            submission_status_changed_tx,
            submission_status_changed_rx,
        }
    }

    /// Unpauses the given submission.
    ///
    /// # Errors
    ///
    /// Returns a database error if the update fails, or `SubmissionNotFound` if the
    /// submission is not paused.
    pub async fn unpause_submission(
        &self,
        id: SubmissionId,
        conn: impl WriterConnection,
    ) -> Result<(), E<DatabaseError, SubmissionNotFound>> {
        unpause_submission(
            id,
            conn,
            &self.notify_on_insert,
            &self.submission_status_changed_tx,
        )
        .await
    }

    /// Cancels the given submission.
    ///
    /// # Errors
    ///
    /// Returns a database error if the update fails, `SubmissionNotFound` if the
    /// submission is missing, or `SubmissionNotCancellable` if it cannot be cancelled.
    pub async fn cancel_submission(
        &self,
        id: SubmissionId,
        conn: impl WriterConnection,
    ) -> Result<(), E![DatabaseError, SubmissionNotFound, SubmissionNotCancellable]> {
        cancel_submission(id, conn, &self.submission_status_changed_tx).await
    }

    /// Gets the current status for the given submission.
    ///
    /// # Errors
    ///
    /// Returns a database error if the lookup fails.
    pub async fn submission_status(
        &self,
        id: SubmissionId,
        mut conn: impl Connection,
    ) -> Result<Option<SubmissionStatus>, DatabaseError> {
        submission_status(id, &mut conn).await
    }
}

impl Clone for CoreApi {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            notify_on_insert: self.notify_on_insert.clone(),
            submission_status_changed_tx: self.submission_status_changed_tx.clone(),
            submission_status_changed_rx: self.submission_status_changed_rx.resubscribe(),
        }
    }
}
