use crate::E;
use crate::common::errors::{DatabaseError, E, SubmissionNotCancellable, SubmissionNotFound};
use crate::common::submission::db::{cancel_submission, submission_status, unpause_submission};
use crate::common::submission::{SubmissionId, SubmissionStatus};
use crate::db;
use crate::db::{Connection, DBPools, WriterConnection};
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use tokio::sync::Notify;

#[async_trait]
pub trait Extension: Send + Sync {
    /// Returns whether the extension references the given submission.
    /// Used to e.g. check if old submissions can be deleted safely.
    async fn references_submission(
        &self,
        submission: SubmissionId,
        conn: &mut db::conn::Writer<db::conn::NoTransaction>,
    ) -> sqlx::Result<bool>;

    /// Allows the Extension to register HTTP handlers.
    /// Defaults to not registering anything.
    fn bind_router(&self, router: Router) -> Router {
        router
    }
}

#[derive(Debug, Clone)]
pub struct CoreApi {
    pub pool: DBPools,
    notify_on_insert: Arc<Notify>,
    submission_status_changed_tx: tokio::sync::broadcast::Sender<SubmissionId>,
}

impl CoreApi {
    #[must_use]
    pub fn new(
        pool: DBPools,
        notify_on_insert: Arc<Notify>,
        submission_status_changed_tx: tokio::sync::broadcast::Sender<SubmissionId>,
    ) -> Self {
        Self {
            pool,
            notify_on_insert,
            submission_status_changed_tx,
        }
    }

    /// Subscribe before starting a task that processes submission status changes.
    #[must_use]
    pub fn subscribe_submission_status_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<SubmissionId> {
        self.submission_status_changed_tx.subscribe()
    }

    pub(crate) fn notify_submission_status_changed(&self, id: SubmissionId) {
        let _ = self.submission_status_changed_tx.send(id);
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
