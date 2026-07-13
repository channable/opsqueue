use std::hint::spin_loop;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const TIMESTAMP_BITS: u64 = 42;
const INSTANCE_BITS: u64 = 10;
const SEQUENCE_BITS: u64 = 12;
const TIMESTAMP_SHIFT: u64 = INSTANCE_BITS + SEQUENCE_BITS;
const SEQUENCE_MASK: u64 = (1 << SEQUENCE_BITS) - 1;
const INSTANCE_MASK: u64 = (1 << INSTANCE_BITS) - 1;
const SUBMISSION_SNOWFLAKE_INSTANCE: u64 = 0;

/// Holds the most recently generated raw snowflake value.
///
/// This stores packed snowflake components in the exact same layout used by
/// `snowflaked` for `u64`: 42-bit timestamp, 10-bit instance, 12-bit sequence.
static LAST_SUBMISSION_ID: AtomicU64 = AtomicU64::new(0);

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is set before the Unix epoch")
        .as_millis() as u64
}

fn pack_submission_snowflake(timestamp_ms: u64, instance: u64, sequence: u64) -> u64 {
    debug_assert!(timestamp_ms < (1 << TIMESTAMP_BITS));
    debug_assert!(instance <= INSTANCE_MASK);
    debug_assert!(sequence <= SEQUENCE_MASK);
    (timestamp_ms << TIMESTAMP_SHIFT) | (instance << SEQUENCE_BITS) | sequence
}

pub(crate) fn snowflake_timestamp_ms(snowflake: u64) -> u64 {
    snowflake >> TIMESTAMP_SHIFT
}

#[cfg(test)]
fn snowflake_instance(snowflake: u64) -> u64 {
    (snowflake >> SEQUENCE_BITS) & INSTANCE_MASK
}

fn snowflake_sequence(snowflake: u64) -> u64 {
    snowflake & SEQUENCE_MASK
}

#[cfg(test)]
fn wait_until_next_millisecond_with<Now, Tick>(
    last_millisecond: u64,
    now_ms: &Now,
    tick_wait: &Tick,
) -> u64
where
    Now: Fn() -> u64,
    Tick: Fn(),
{
    loop {
        let now = now_ms();
        if now > last_millisecond {
            return now;
        }
        tick_wait();
    }
}

fn next_submission_snowflake<Wait>(last: u64, now_ms_value: u64, wait_until_next_ms: Wait) -> u64
where
    Wait: FnOnce(u64) -> u64,
{
    let last_timestamp_ms = snowflake_timestamp_ms(last);
    let last_sequence = snowflake_sequence(last);

    if now_ms_value < last_timestamp_ms {
        panic!("Clock has moved backwards! This is not supported");
    } else if now_ms_value > last_timestamp_ms {
        pack_submission_snowflake(now_ms_value, SUBMISSION_SNOWFLAKE_INSTANCE, 0)
    } else if last_sequence < SEQUENCE_MASK {
        pack_submission_snowflake(
            now_ms_value,
            SUBMISSION_SNOWFLAKE_INSTANCE,
            last_sequence + 1,
        )
    } else {
        let next_ms = wait_until_next_ms(last_timestamp_ms);
        pack_submission_snowflake(next_ms, SUBMISSION_SNOWFLAKE_INSTANCE, 0)
    }
}

#[cfg(test)]
fn generate_submission_snowflake_with<Now, Tick>(
    state: &AtomicU64,
    now_ms: Now,
    tick_wait: Tick,
) -> u64
where
    Now: Fn() -> u64,
    Tick: Fn(),
{
    loop {
        let last = state.load(Ordering::Relaxed);
        let now_ms_value = now_ms();
        let next = next_submission_snowflake(last, now_ms_value, |last_ms| {
            wait_until_next_millisecond_with(last_ms, &now_ms, &tick_wait)
        });

        if state
            .compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return next;
        }
    }
}

fn wait_until_next_millisecond(last_millisecond: u64) -> u64 {
    loop {
        let now = now_unix_ms();
        if now > last_millisecond {
            return now;
        }
        spin_loop();
    }
}

pub(crate) fn generate_submission_snowflake() -> u64 {
    loop {
        let last = LAST_SUBMISSION_ID.load(Ordering::Relaxed);
        let now_ms_value = now_unix_ms();
        let next = next_submission_snowflake(last, now_ms_value, wait_until_next_millisecond);

        if LAST_SUBMISSION_ID
            .compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::submission::SubmissionId;
    use proptest::prelude::*;
    use snowflaked::Snowflake;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::{Duration, SystemTime};
    use ux::u63;

    fn timestamp_strategy() -> BoxedStrategy<u64> {
        let max = (1_u64 << TIMESTAMP_BITS) - 1;
        prop_oneof![
            Just(0_u64),
            Just(1_u64),
            Just(2_u64),
            Just(max / 2),
            Just(max.saturating_sub(2)),
            Just(max.saturating_sub(1)),
            Just(max),
            0_u64..=max,
        ]
        .boxed()
    }

    fn instance_strategy() -> BoxedStrategy<u64> {
        prop_oneof![
            Just(0_u64),
            Just(1_u64),
            Just(INSTANCE_MASK.saturating_sub(1)),
            Just(INSTANCE_MASK),
            0_u64..=INSTANCE_MASK,
        ]
        .boxed()
    }

    fn sequence_strategy() -> BoxedStrategy<u64> {
        prop_oneof![
            Just(0_u64),
            Just(1_u64),
            Just(SEQUENCE_MASK.saturating_sub(1)),
            Just(SEQUENCE_MASK),
            0_u64..=SEQUENCE_MASK,
        ]
        .boxed()
    }

    #[derive(Clone, Copy, Debug)]
    struct SnowflakeModel {
        last: u64,
    }

    impl SnowflakeModel {
        fn new(last: u64) -> Self {
            Self { last }
        }

        fn set_last(&mut self, last: u64) {
            self.last = last;
        }

        fn next_with_tick(&mut self, now_ms: u64, tick_to: Option<u64>) -> u64 {
            let last_timestamp_ms = snowflake_timestamp_ms(self.last);
            let last_sequence = snowflake_sequence(self.last);

            let next = if now_ms < last_timestamp_ms {
                panic!("Clock has moved backwards! This is not supported");
            } else if now_ms > last_timestamp_ms {
                pack_submission_snowflake(now_ms, SUBMISSION_SNOWFLAKE_INSTANCE, 0)
            } else if last_sequence < SEQUENCE_MASK {
                pack_submission_snowflake(now_ms, SUBMISSION_SNOWFLAKE_INSTANCE, last_sequence + 1)
            } else {
                let next_ms = tick_to.expect("tick_to must be provided on sequence overflow");
                assert!(
                    next_ms > last_timestamp_ms,
                    "tick_to must advance the millisecond"
                );
                pack_submission_snowflake(next_ms, SUBMISSION_SNOWFLAKE_INSTANCE, 0)
            };

            self.last = next;
            next
        }
    }

    #[test]
    fn ids_are_unique_and_strictly_monotonic() {
        let mut seen = HashSet::new();
        let mut previous = SubmissionId::new();
        assert!(seen.insert(previous));

        for _ in 0..100_000 {
            let current = SubmissionId::new();
            assert!(
                current > previous,
                "expected {current:?} to be strictly greater than {previous:?}"
            );
            assert!(
                seen.insert(current),
                "generated a duplicate id: {current:?}"
            );
            previous = current;
        }
    }

    #[test]
    fn system_time_is_close_to_now_and_uses_unix_epoch() {
        let before = SystemTime::now();
        let id = SubmissionId::new();
        let after = SystemTime::now();

        let generated = id.system_time();

        // The embedded timestamp has millisecond resolution, so allow the
        // reconstructed time to sit anywhere within the surrounding window
        // (truncated down to the millisecond) plus a small slack.
        let lower = before - Duration::from_millis(1);
        let upper = after + Duration::from_millis(1);
        assert!(
            generated >= lower && generated <= upper,
            "generated time {generated:?} not within [{lower:?}, {upper:?}]"
        );
    }

    #[test]
    fn ids_are_unique_across_threads() {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| (0..10_000).map(|_| SubmissionId::new()).collect::<Vec<_>>())
            })
            .collect();

        let mut seen = HashSet::new();
        for thread in threads {
            for id in thread.join().expect("worker thread panicked") {
                assert!(
                    seen.insert(id),
                    "generated a duplicate id across threads: {id:?}"
                );
            }
        }
    }

    #[test]
    fn packed_layout_matches_snowflaked_u64_layout() {
        let id = pack_submission_snowflake(123_456, 17, 3210);
        assert_eq!(snowflake_timestamp_ms(id), 123_456);
        assert_eq!(snowflake_instance(id), 17);
        assert_eq!(snowflake_sequence(id), 3210);
    }

    #[test]
    fn generated_id_is_compatible_with_snowflaked_shape() {
        static REFERENCE_GENERATOR: snowflaked::sync::Generator =
            snowflaked::sync::Generator::new(0);

        let ours: u64 = SubmissionId::new().into();
        let theirs: u64 = REFERENCE_GENERATOR.generate();

        // Both IDs must use the same bit partitioning and instance value.
        assert_eq!(snowflake_instance(ours), 0);
        assert_eq!(snowflake_instance(theirs), theirs.instance());
        assert_eq!(snowflake_sequence(theirs), theirs.sequence());
        assert_eq!(snowflake_timestamp_ms(theirs), theirs.timestamp());

        // Generated close in time: timestamps should be very close, though not necessarily equal.
        let ours_ts = snowflake_timestamp_ms(ours);
        let theirs_ts = theirs.timestamp();
        let diff = ours_ts.abs_diff(theirs_ts);
        assert!(
            diff <= 5,
            "expected close timestamps, got ours={ours_ts}ms theirs={theirs_ts}ms"
        );
    }

    #[test]
    fn sequence_overflow_waits_next_millisecond_and_resets_to_zero() {
        let start_ms = 42_000;
        let state = AtomicU64::new(pack_submission_snowflake(
            start_ms,
            SUBMISSION_SNOWFLAKE_INSTANCE,
            SEQUENCE_MASK,
        ));
        let now = AtomicU64::new(start_ms);
        let advanced = AtomicBool::new(false);

        let generated = generate_submission_snowflake_with(
            &state,
            || now.load(Ordering::Relaxed),
            || {
                if !advanced.swap(true, Ordering::Relaxed) {
                    now.store(start_ms + 1, Ordering::Relaxed);
                }
            },
        );

        assert_eq!(snowflake_timestamp_ms(generated), start_ms + 1);
        assert_eq!(snowflake_instance(generated), SUBMISSION_SNOWFLAKE_INSTANCE);
        assert_eq!(snowflake_sequence(generated), 0);
    }

    #[test]
    fn generated_ids_always_have_valid_components() {
        let mut previous_timestamp = 0;

        for _ in 0..100_000 {
            let id: u64 = SubmissionId::new().into();
            let timestamp = snowflake_timestamp_ms(id);
            let instance = snowflake_instance(id);
            let sequence = snowflake_sequence(id);

            assert_eq!(instance, SUBMISSION_SNOWFLAKE_INSTANCE);
            assert!(sequence <= SEQUENCE_MASK);
            assert!(timestamp >= previous_timestamp);
            previous_timestamp = timestamp;
        }
    }

    #[test]
    fn high_volume_multithreaded_generation_has_no_duplicates_after_sort() {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..25_000)
                        .map(|_| u64::from(SubmissionId::new()))
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let mut all_ids = Vec::with_capacity(8 * 25_000);
        for thread in threads {
            all_ids.extend(thread.join().expect("worker thread panicked"));
        }

        all_ids.sort_unstable();
        for pair in all_ids.windows(2) {
            assert!(
                pair[0] < pair[1],
                "duplicate or non-increasing ids: {:?}",
                pair
            );
        }
    }

    #[test]
    fn submission_id_json_roundtrip_preserves_value() {
        let original = SubmissionId::new();
        let json = serde_json::to_string(&original).expect("serialization failed");
        let decoded: SubmissionId = serde_json::from_str(&json).expect("deserialization failed");
        assert_eq!(decoded, original);
    }

    #[test]
    fn ids_across_millisecond_boundary_keep_order() {
        let id_before = SubmissionId::new();
        let before_ts = snowflake_timestamp_ms(id_before.into());

        loop {
            let now_ms = now_unix_ms();
            if now_ms > before_ts {
                break;
            }
            spin_loop();
        }

        let id_after = SubmissionId::new();
        let after_ts = snowflake_timestamp_ms(id_after.into());

        assert!(id_after > id_before);
        assert!(after_ts >= before_ts);
    }

    #[test]
    #[should_panic(expected = "Clock has moved backwards! This is not supported")]
    fn clock_rollback_panics_like_snowflaked() {
        let state = AtomicU64::new(pack_submission_snowflake(
            10_000,
            SUBMISSION_SNOWFLAKE_INSTANCE,
            3,
        ));
        let _ = generate_submission_snowflake_with(&state, || 9_999, || {});
    }

    #[test]
    fn pack_matches_snowflaked_from_parts_bit_for_bit() {
        let timestamps = [0_u64, 1, 42, 1_234_567, (1 << 20) - 1, (1 << 30) - 1];
        let sequences = [0_u64, 1, 7, 127, 2048, SEQUENCE_MASK];

        for timestamp in timestamps {
            for sequence in sequences {
                let ours =
                    pack_submission_snowflake(timestamp, SUBMISSION_SNOWFLAKE_INSTANCE, sequence);
                let theirs = <u64 as Snowflake>::from_parts(
                    timestamp,
                    SUBMISSION_SNOWFLAKE_INSTANCE,
                    sequence,
                );
                assert_eq!(
                    ours, theirs,
                    "bit mismatch for ts={timestamp}, seq={sequence}: ours={ours}, theirs={theirs}"
                );
            }
        }
    }

    #[test]
    fn decoder_matches_snowflaked_trait_accessors() {
        for _ in 0..50_000 {
            let id: u64 = SubmissionId::new().into();
            assert_eq!(snowflake_timestamp_ms(id), id.timestamp());
            assert_eq!(snowflake_instance(id), id.instance());
            assert_eq!(snowflake_sequence(id), id.sequence());
        }
    }

    #[test]
    fn repeated_multithreaded_stress_has_no_duplicates() {
        const ROUNDS: usize = 20;
        const THREADS: usize = 6;
        const IDS_PER_THREAD: usize = 6_000;

        for _ in 0..ROUNDS {
            let threads: Vec<_> = (0..THREADS)
                .map(|_| {
                    std::thread::spawn(|| {
                        (0..IDS_PER_THREAD)
                            .map(|_| u64::from(SubmissionId::new()))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();

            let mut ids = Vec::with_capacity(THREADS * IDS_PER_THREAD);
            for thread in threads {
                ids.extend(thread.join().expect("worker thread panicked"));
            }

            ids.sort_unstable();
            for pair in ids.windows(2) {
                assert!(
                    pair[0] < pair[1],
                    "duplicate or non-increasing ids: {:?}",
                    pair
                );
            }
        }
    }

    #[test]
    fn submission_id_u63_boundary_is_handled_correctly() {
        let max_u63: u64 = u63::MAX.into();
        let max_compatible_timestamp = max_u63 >> TIMESTAMP_SHIFT;

        let max_compatible_id =
            pack_submission_snowflake(max_compatible_timestamp, SUBMISSION_SNOWFLAKE_INSTANCE, 0);
        let parsed = SubmissionId::try_from(max_compatible_id)
            .expect("max compatible value should be accepted");
        assert_eq!(u64::from(parsed), max_compatible_id);

        let above_boundary_id = pack_submission_snowflake(
            max_compatible_timestamp + 1,
            SUBMISSION_SNOWFLAKE_INSTANCE,
            0,
        );
        assert!(
            SubmissionId::try_from(above_boundary_id).is_err(),
            "value above u63 boundary should be rejected"
        );
    }

    #[test]
    fn model_based_scripted_transitions_match_generator() {
        let state = AtomicU64::new(0);
        let mut model = SnowflakeModel::new(0);

        let scripted_steps = [
            (1_000_u64, None),
            (1_000_u64, None),
            (1_001_u64, None),
            (1_001_u64, None),
            (1_050_u64, None),
        ];

        for (now_ms, tick_to) in scripted_steps {
            let current = AtomicU64::new(now_ms);
            let generated = generate_submission_snowflake_with(
                &state,
                || current.load(Ordering::Relaxed),
                || {
                    if let Some(next_ms) = tick_to {
                        current.store(next_ms, Ordering::Relaxed);
                    } else {
                        panic!("unexpected wait in non-overflow scripted step");
                    }
                },
            );
            let expected = model.next_with_tick(now_ms, tick_to);
            assert_eq!(generated, expected);
            assert_eq!(state.load(Ordering::Relaxed), expected);
        }

        let near_overflow =
            pack_submission_snowflake(9_000, SUBMISSION_SNOWFLAKE_INSTANCE, SEQUENCE_MASK - 1);
        state.store(near_overflow, Ordering::Relaxed);
        model.set_last(near_overflow);

        let current = AtomicU64::new(9_000);
        let generated_max = generate_submission_snowflake_with(
            &state,
            || current.load(Ordering::Relaxed),
            || panic!("unexpected wait before reaching sequence max"),
        );
        let expected_max = model.next_with_tick(9_000, None);
        assert_eq!(generated_max, expected_max);
        assert_eq!(snowflake_sequence(generated_max), SEQUENCE_MASK);

        let current = AtomicU64::new(9_000);
        let ticked = AtomicBool::new(false);
        let generated_after_wrap = generate_submission_snowflake_with(
            &state,
            || current.load(Ordering::Relaxed),
            || {
                if !ticked.swap(true, Ordering::Relaxed) {
                    current.store(9_001, Ordering::Relaxed);
                }
            },
        );
        let expected_after_wrap = model.next_with_tick(9_000, Some(9_001));
        assert_eq!(generated_after_wrap, expected_after_wrap);
        assert_eq!(snowflake_timestamp_ms(generated_after_wrap), 9_001);
        assert_eq!(snowflake_sequence(generated_after_wrap), 0);
    }

    #[test]
    fn clock_rollback_failure_mode_matches_oracle() {
        let ours = std::panic::catch_unwind(|| {
            let state = AtomicU64::new(pack_submission_snowflake(
                10_000,
                SUBMISSION_SNOWFLAKE_INSTANCE,
                3,
            ));
            let _ = generate_submission_snowflake_with(&state, || 9_999, || {});
        });
        assert!(
            ours.is_err(),
            "expected custom generator to panic on rollback"
        );

        let oracle = std::panic::catch_unwind(|| {
            let future_epoch = SystemTime::now()
                .checked_add(Duration::from_millis(100))
                .expect("future epoch should fit in SystemTime");
            let generator: snowflaked::sync::Generator = snowflaked::sync::Generator::builder()
                .instance(0)
                .epoch(future_epoch)
                .build();
            let _: u64 = generator.generate();
        });
        assert!(
            oracle.is_err(),
            "expected snowflaked generator to panic on rollback-like time failure"
        );
    }

    proptest! {
        #[test]
        fn prop_pack_unpack_roundtrip_preserves_components(
            timestamp in timestamp_strategy(),
            instance in instance_strategy(),
            sequence in sequence_strategy(),
        ) {
            let packed = pack_submission_snowflake(timestamp, instance, sequence);
            prop_assert_eq!(snowflake_timestamp_ms(packed), timestamp);
            prop_assert_eq!(snowflake_instance(packed), instance);
            prop_assert_eq!(snowflake_sequence(packed), sequence);
        }

        #[test]
        fn prop_pack_matches_snowflaked_oracle(
            timestamp in timestamp_strategy(),
            instance in instance_strategy(),
            sequence in sequence_strategy(),
        ) {
            let ours = pack_submission_snowflake(timestamp, instance, sequence);
            let oracle = <u64 as Snowflake>::from_parts(timestamp, instance, sequence);
            prop_assert_eq!(ours, oracle);
        }

        #[test]
        fn prop_submission_ids_are_monotonic_and_unique(
            sample_size in 2_usize..1_000
        ) {
            let mut previous = SubmissionId::new();
            let mut seen = HashSet::with_capacity(sample_size);
            prop_assert!(seen.insert(u64::from(previous)));

            for _ in 1..sample_size {
                let current = SubmissionId::new();
                prop_assert!(current > previous);
                prop_assert!(seen.insert(u64::from(current)));
                previous = current;
            }
        }

        #[test]
        fn prop_submission_id_try_from_u64_boundary_behavior(
            timestamp in timestamp_strategy(),
            sequence in sequence_strategy(),
        ) {
            let packed = pack_submission_snowflake(timestamp, SUBMISSION_SNOWFLAKE_INSTANCE, sequence);
            let should_fit_u63 = packed <= u64::from(u63::MAX);
            let parsed = SubmissionId::try_from(packed);

            prop_assert_eq!(parsed.is_ok(), should_fit_u63);
            if let Ok(parsed) = parsed {
                prop_assert_eq!(u64::from(parsed), packed);
            }
        }
    }
}
