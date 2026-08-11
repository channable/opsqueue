use crate::common::extension::{CoreApi, Extension};
use crate::common::submission::SubmissionId;
use crate::db::Connection;
use crate::db::conn::{NoTransaction, Writer};
use crate::delegation::server::ServerState;
use async_trait::async_trait;
use axum::Router;
use sqlx::query_scalar;
use tokio_util::sync::CancellationToken;

pub struct DelegationExtension {
    cancellation_token: CancellationToken,
    core_api: CoreApi,
    delegation_server_url: url::Url,
}

impl DelegationExtension {
    #[must_use]
    pub fn new(
        cancellation_token: CancellationToken,
        core_api: CoreApi,
        delegation_server_url: url::Url,
    ) -> Self {
        DelegationExtension {
            cancellation_token,
            core_api,
            delegation_server_url,
        }
    }
}
#[async_trait]
impl Extension for DelegationExtension {
    async fn references_submission(
        &self,
        submission: SubmissionId,
        conn: &mut Writer<NoTransaction>,
    ) -> sqlx::Result<bool> {
        query_scalar!(
            r#"
            SELECT EXISTS(SELECT 1 FROM submissions_external_task WHERE submission_id = $1) AS "exists: bool"
            "#,
            submission
        )
            .fetch_one(conn.get_inner())
            .await
    }

    fn bind_router(&self, router: Router) -> Router {
        let delegation_routes = ServerState::new(
            self.cancellation_token.clone(),
            self.core_api.clone(),
            self.delegation_server_url.clone(),
        )
        .run_background()
        .build_router();

        router.nest("/delegation", delegation_routes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::StrategicMetadataMap;
    use crate::common::chunk::ChunkSize;
    use crate::common::submission::InitialSubmissionStatus;
    use crate::common::submission::db::{
        count_submissions_completed, insert_submission_from_chunks, periodically_cleanup_old,
        submission_status,
    };
    use crate::db::DBPools;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Notify, broadcast};

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn periodic_cleanup_respects_external_task_references(
        pool_opts: sqlx::pool::PoolOptions<sqlx::Sqlite>,
        conn_opts: sqlx::sqlite::SqliteConnectOptions,
    ) {
        let reader_pool = pool_opts
            .clone()
            .max_connections(16)
            .connect_with(conn_opts.clone())
            .await
            .unwrap();
        let writer_pool = pool_opts
            .max_connections(1)
            .connect_with(conn_opts)
            .await
            .unwrap();
        let db = DBPools::from_test_pools(&reader_pool, &writer_pool);

        let (referenced, unreferenced) = {
            let mut conn = db.writer_conn().await.unwrap();
            let referenced = insert_submission_from_chunks(
                None,
                vec![],
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                InitialSubmissionStatus::InProgress,
                &mut conn,
            )
            .await
            .unwrap();
            let unreferenced = insert_submission_from_chunks(
                None,
                vec![],
                None,
                StrategicMetadataMap::default(),
                ChunkSize::default(),
                InitialSubmissionStatus::InProgress,
                &mut conn,
            )
            .await
            .unwrap();

            sqlx::query!(
                "INSERT INTO submissions_external_task (submission_id, task_id) VALUES ($1, 'task')",
                referenced,
            )
            .execute(conn.get_inner())
            .await
            .unwrap();
            sqlx::query!(
                "UPDATE submissions_completed SET completed_at = julianday('now', '-1 day')"
            )
            .execute(conn.get_inner())
            .await
            .unwrap();

            (referenced, unreferenced)
        };

        let (sender, _) = broadcast::channel(1);
        let extensions: Vec<Box<dyn Extension>> = vec![Box::new(DelegationExtension::new(
            CancellationToken::new(),
            CoreApi::new(db.clone(), Arc::new(Notify::new()), sender),
            "http://localhost/".parse().unwrap(),
        ))];
        let cleanup_db = db.clone();
        let cleanup = tokio::spawn(async move {
            periodically_cleanup_old(&cleanup_db, Duration::ZERO, &extensions).await;
        });

        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut conn = db.reader_conn().await.unwrap();
                let remaining = count_submissions_completed(&mut conn).await.unwrap();
                assert!(remaining > 0, "cleanup deleted the referenced submission");
                if remaining == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        cleanup.abort();
        assert!(cleanup.await.unwrap_err().is_cancelled());
        result.expect("periodic cleanup did not delete the unreferenced submission");

        let mut conn = db.reader_conn().await.unwrap();
        assert!(
            submission_status(referenced, &mut conn)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            submission_status(unreferenced, &mut conn)
                .await
                .unwrap()
                .is_none()
        );
    }
}
