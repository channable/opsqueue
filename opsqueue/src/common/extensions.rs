use crate::common::errors::DatabaseError;
use crate::common::submission::db::unpause_submission;
use crate::common::submission::{SubmissionId, SubmissionStatus};
use futures::Stream;

pub struct ExtensionInterface {
    submission_status_changed: tokio::sync::mpsc::UnboundedReceiver<SubmissionId>,
}

impl ExtensionInterface {
    /// Unpauses the given submissions atomically (i.e. within a transaction).
    pub async fn unpause_submissions(
        ids: impl Iterator<Item = SubmissionId>,
    ) -> Result<(), DatabaseError> {
        unpause_submission()
    }

    /// Cancels the given submissions atomically (i.e. within a transaction).
    pub async fn cancel_submissions(
        ids: impl Iterator<Item = SubmissionId>,
    ) -> Result<(), DatabaseError> {
        todo!()
    }

    /// Gets the current status for the given submissions
    pub fn get_submission_statuses<'a>(
        ids: impl Iterator<Item = SubmissionId>,
    ) -> impl Stream<Item = Result<SubmissionStatus, DatabaseError>> {
        todo!()
    }

    /// Waits until any submissions status was changed. Returns the id of the changed submission.
    /// Returns None if the underlying channel was closed (i.e. the server is shutting down).
    pub async fn wait_for_submission_status_change(&mut self) -> Option<SubmissionId> {
        self.submission_status_changed.recv().await
    }
}
