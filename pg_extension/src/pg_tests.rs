use pgrx::prelude::*;

#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn scalar_i64(query: &str) -> i64 {
        Spi::get_one::<i64>(query)
            .expect("PostgreSQL query should succeed")
            .expect("query should return a bigint")
    }

    fn scalar_bool(query: &str) -> bool {
        Spi::get_one::<bool>(query)
            .expect("PostgreSQL query should succeed")
            .expect("query should return a boolean")
    }

    fn submit_one_chunk() -> i64 {
        scalar_i64("SELECT opsqueue.submit(1, p_chunk_contents => ARRAY[decode('01', 'hex')])")
    }

    #[pg_test]
    fn version_is_available() {
        assert_eq!(
            crate::opsqueue::opsqueue_version(),
            env!("CARGO_PKG_VERSION")
        );
    }

    #[pg_test]
    fn submission_reservation_and_completion_round_trip() {
        let submission_id = scalar_i64(
            "SELECT opsqueue.submit(
                1,
                p_prefix => 'pgrx-round-trip',
                p_chunk_contents => ARRAY[decode('01', 'hex')],
                p_trace_context => 'trace-context'
            )",
        );
        let claimed = scalar_i64(
            "SELECT count(*)::bigint
             FROM opsqueue.reserve(
                '00000000-0000-0000-0000-000000000001'::uuid,
                1
             ) AS claim
             WHERE claim.otel_trace_carrier = 'trace-context'",
        );
        assert_eq!(claimed, 1);

        let completed = scalar_bool(&format!(
            "SELECT opsqueue.complete(
                '00000000-0000-0000-0000-000000000001'::uuid,
                {submission_id},
                0,
                decode('ff', 'hex')
            )"
        ));
        assert!(completed);
        assert!(scalar_bool(&format!(
            "SELECT status = 'completed' AND chunks_done = 1 AND chunks_ready = 0
             FROM opsqueue.status({submission_id})"
        )));
        assert!(scalar_bool(&format!(
            "SELECT encode(output_content, 'hex') = 'ff'
             FROM opsqueue.results({submission_id}) WHERE chunk_index = 0"
        )));
        assert_eq!(scalar_i64("SELECT opsqueue.count_chunks()"), 0);
        assert_eq!(scalar_i64("SELECT opsqueue.count_chunks_completed()"), 1);
        assert_eq!(scalar_i64("SELECT opsqueue.count_completed()"), 1);
        assert_eq!(scalar_i64("SELECT opsqueue.count_in_progress()"), 0);
    }

    #[pg_test]
    fn reservations_are_exclusive_and_renewable() {
        let submission_id = submit_one_chunk();
        assert_eq!(
            scalar_i64(
                "SELECT count(*)::bigint FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000002'::uuid, 1
                )"
            ),
            1
        );
        assert_eq!(
            scalar_i64(
                "SELECT count(*)::bigint FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000003'::uuid, 1
                )"
            ),
            0
        );
        assert!(scalar_bool(&format!(
            "SELECT opsqueue.renew(
                '00000000-0000-0000-0000-000000000002'::uuid,
                {submission_id}, 0, interval '20 minutes'
            )"
        )));
    }

    #[pg_test]
    fn oldest_newest_and_random_strategies_claim_work() {
        let oldest_submission = submit_one_chunk();
        let _middle_submission = submit_one_chunk();
        let newest_submission = submit_one_chunk();

        assert_eq!(
            scalar_i64(
                "SELECT submission_id FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000010'::uuid, 1, p_strategy => 'oldest'
                )"
            ),
            oldest_submission
        );
        assert_eq!(
            scalar_i64(
                "SELECT submission_id FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000011'::uuid, 1, p_strategy => 'newest'
                )"
            ),
            newest_submission
        );
        assert_eq!(
            scalar_i64(
                "SELECT count(*)::bigint FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000012'::uuid, 1, p_strategy => 'random'
                )"
            ),
            1
        );
    }

    #[pg_test]
    fn prefer_distinct_spreads_reservations_across_metadata_values() {
        let first_submission = scalar_i64(
            "SELECT opsqueue.submit(1, p_strategic_metadata => '{\"tenant\": 1}'::jsonb)",
        );
        let second_submission = scalar_i64(
            "SELECT opsqueue.submit(1, p_strategic_metadata => '{\"tenant\": 2}'::jsonb)",
        );
        let first_claim = scalar_i64(
            "SELECT submission_id FROM opsqueue.reserve(
                '00000000-0000-0000-0000-000000000008'::uuid,
                1,
                interval '10 minutes',
                'prefer_distinct',
                ARRAY['tenant'],
                'random'
            )",
        );
        let second_claim = scalar_i64(
            "SELECT submission_id FROM opsqueue.reserve(
                '00000000-0000-0000-0000-000000000009'::uuid,
                1,
                interval '10 minutes',
                'prefer_distinct',
                ARRAY['tenant'],
                'random'
            )",
        );

        assert!(first_claim == first_submission || first_claim == second_submission);
        assert!(second_claim == first_submission || second_claim == second_submission);
        assert_ne!(first_claim, second_claim);
    }

    #[pg_test]
    fn expired_reservations_retry_and_eventually_fail() {
        let submission_id = submit_one_chunk();
        let worker_id = "00000000-0000-0000-0000-000000000004";
        assert_eq!(
            scalar_i64(&format!(
                "SELECT count(*)::bigint FROM opsqueue.reserve('{worker_id}'::uuid, 1)"
            )),
            1
        );
        Spi::run(&format!(
            "UPDATE opsqueue.chunks
             SET reserved_until = clock_timestamp() - interval '1 second'
             WHERE submission_id = {submission_id} AND chunk_index = 0"
        ))
        .expect("reservation expiry update should succeed");

        assert_eq!(scalar_i64("SELECT opsqueue.reap_expired(2, 10)"), 1);
        assert!(scalar_bool(&format!(
            "SELECT retries = 1 AND status = 'pending'
             FROM opsqueue.chunks WHERE submission_id = {submission_id} AND chunk_index = 0"
        )));

        assert_eq!(
            scalar_i64(&format!(
                "SELECT count(*)::bigint FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000005'::uuid, 1
                )"
            )),
            1
        );
        assert!(scalar_bool(&format!(
            "SELECT opsqueue.fail(
                '00000000-0000-0000-0000-000000000005'::uuid,
                {submission_id}, 0, 'failed again', 2
            )"
        )));
        assert!(scalar_bool(&format!(
            "SELECT status = 'failed' FROM opsqueue.status({submission_id})"
        )));
        assert_eq!(scalar_i64("SELECT opsqueue.count_failed()"), 1);
        assert_eq!(
            scalar_i64(&format!(
                "SELECT count(*)::bigint FROM opsqueue.failure_history({submission_id})"
            )),
            2
        );
    }

    #[pg_test]
    fn paused_submission_can_be_unpaused_and_cancelled() {
        let submission_id = scalar_i64(
            "SELECT opsqueue.submit(1, p_prefix => 'cancelled-prefix', p_initial_status => 'paused')",
        );
        assert!(scalar_bool(&format!(
            "SELECT chunks_ready = 0 FROM opsqueue.status({submission_id})"
        )));
        assert_eq!(scalar_i64("SELECT opsqueue.count_paused()"), 1);
        assert_eq!(
            scalar_i64(
                "SELECT count(*)::bigint FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000006'::uuid, 1
                )"
            ),
            0
        );
        assert!(scalar_bool(&format!(
            "SELECT opsqueue.unpause({submission_id})"
        )));
        assert_eq!(
            scalar_i64(&format!(
                "SELECT count(*)::bigint FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000006'::uuid, 1
                )"
            )),
            1
        );
        assert!(scalar_bool(&format!(
            "SELECT opsqueue.cancel({submission_id})"
        )));
        assert!(scalar_bool(&format!(
            "SELECT status = 'cancelled' FROM opsqueue.status({submission_id})"
        )));
        assert_eq!(scalar_i64("SELECT opsqueue.count_cancelled()"), 1);
        assert_eq!(scalar_i64("SELECT opsqueue.count_paused()"), 0);
        assert_eq!(
            Spi::get_one::<i64>("SELECT opsqueue.lookup_by_prefix('cancelled-prefix')")
                .expect("prefix lookup should succeed"),
            None
        );
        assert!(!scalar_bool(&format!(
            "SELECT opsqueue.complete(
                '00000000-0000-0000-0000-000000000006'::uuid,
                {submission_id}, 0
            )"
        )));
    }

    #[pg_test]
    fn metadata_lookup_only_returns_matching_in_progress_submissions() {
        let matching_id =
            scalar_i64("SELECT opsqueue.submit(1, p_strategic_metadata => '{\"team\": 7}'::jsonb)");
        let _other_id =
            scalar_i64("SELECT opsqueue.submit(1, p_strategic_metadata => '{\"team\": 8}'::jsonb)");
        let _paused_id = scalar_i64(
            "SELECT opsqueue.submit(
                1,
                p_strategic_metadata => '{\"team\": 7}'::jsonb,
                p_initial_status => 'paused'
            )",
        );
        assert_eq!(
            scalar_i64(
                "SELECT count(*)::bigint
                 FROM opsqueue.lookup_by_metadata('{\"team\": 7}'::jsonb, 10)"
            ),
            1
        );
        assert!(scalar_bool(&format!(
            "SELECT EXISTS(
                SELECT 1 FROM opsqueue.lookup_by_metadata('{{\"team\": 7}}'::jsonb, 10) AS matches(id)
                WHERE id = {matching_id}
            )"
        )));
        assert_eq!(
            scalar_i64("SELECT count(*)::bigint FROM opsqueue.lookup_by_metadata('{}'::jsonb, 10)"),
            2
        );
    }

    #[pg_test]
    fn cleanup_removes_old_terminal_rows_and_cascades_chunks() {
        let submission_id = submit_one_chunk();
        assert_eq!(
            scalar_i64(
                "SELECT count(*)::bigint FROM opsqueue.reserve(
                    '00000000-0000-0000-0000-000000000007'::uuid, 1
                )"
            ),
            1
        );
        assert!(scalar_bool(&format!(
            "SELECT opsqueue.complete(
                '00000000-0000-0000-0000-000000000007'::uuid,
                {submission_id}, 0
            )"
        )));
        let paused_id = scalar_i64("SELECT opsqueue.submit(1, p_initial_status => 'paused')");
        Spi::run(&format!(
            "UPDATE opsqueue.submissions
             SET completed_at = clock_timestamp() - interval '2 hours'
             WHERE id = {submission_id}"
        ))
        .expect("terminal timestamp update should succeed");

        assert_eq!(scalar_i64("SELECT opsqueue.cleanup(clock_timestamp())"), 1);
        assert_eq!(
            scalar_i64(&format!(
                "SELECT count(*)::bigint FROM opsqueue.chunks WHERE submission_id = {submission_id}"
            )),
            0
        );
        assert_eq!(
            scalar_i64(&format!(
                "SELECT count(*)::bigint FROM opsqueue.submissions WHERE id = {paused_id}"
            )),
            1
        );
    }
}
