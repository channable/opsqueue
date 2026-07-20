"""Benchmark & fairness harness for the ``PreferDistinct`` selection strategy.

Produces a single figure (one subplot per scenario, a line per strategy) characterising
how reservation latency scales, and checks that work is shared fairly across companies.

The probe reserves every chunk one at a time *without* completing (timing each
``reserve_chunks`` call) and only completes at the end. Reserved chunks stay in the
``chunks`` table until completed, so the reserved set grows by one each iteration: the
x-axis is "chunks already reserved", the y-axis is "time the next reservation took".

Scenarios sweep two axes:
  * backlog shape -- ``few_submissions_many_chunks`` (few companies, many chunks each)
    vs ``many_submissions_few_chunks`` (many companies, one chunk each; stresses
    PreferDistinct's per-submission ranking).
  * insert order -- whale_first vs whale_last; a fair strategy must not depend on it.

Note: absolute times are dominated by WebSocket round-trips, so trust the *shape* of
each curve and the fairness of the interleaving, not the raw milliseconds. Property
checks are non-fatal (recorded and printed at the end); the benchmark characterises
behaviour rather than gating CI. On master the fairness check flags
``few_submissions_many_chunks`` -- that is the bug the fast-and-fair rewrite fixes.
"""

from __future__ import annotations

import csv
import os
import statistics
import time
from collections import Counter
from dataclasses import dataclass
from itertools import batched
from typing import Generator

import matplotlib

matplotlib.use("Agg")  # Headless: never try to open a window in CI.
import matplotlib.pyplot as plt
import pytest

from opsqueue.producer import ProducerClient, SubmissionId
from opsqueue.consumer import ConsumerClient, Chunk
from conftest import (
    background_process,
    opsqueue_service,
    OpsqueueProcess,
    strategy_from_description,
    StrategyDescription,
)

# Non-fatal property violations are collected here and printed at the end of the run.
_FAILURES: list[str] = []


def fail(message: str) -> None:
    _FAILURES.append(message)


FEW_SUBMISSIONS_MANY_CHUNKS = [10**n for n in range(4)]  # chunks per company
MANY_SUBMISSIONS_FEW_CHUNKS = [1] * 300  # 300 companies, one chunk each

BACKLOG_SHAPES: dict[str, list[int]] = {
    "few_submissions_many_chunks": FEW_SUBMISSIONS_MANY_CHUNKS,
    "many_submissions_few_chunks": MANY_SUBMISSIONS_FEW_CHUNKS,
}

# Strategies overlaid within each subplot: (label, description).
STRATEGIES: list[tuple[str, StrategyDescription]] = [
    ("Random", "Random"),
    ("PreferDistinct(company_id, Oldest)", ("PreferDistinct", "company_id", "Oldest")),
]

OUTPUT_DIR = "/tmp/opsqueue_prefer_distinct_benchmark"
COMBINED_FIGURE = os.path.join(OUTPUT_DIR, "prefer_distinct_benchmark.png")

# The reserve-without-complete probe can occupy a single query for tens of seconds at
# large N, so we run opsqueue with a generous heartbeat budget to avoid the client
# tearing down the connection mid-request.
BENCHMARK_HEARTBEAT_INTERVAL = "60 seconds"
BENCHMARK_MAX_MISSABLE_HEARTBEATS = "60"


@pytest.fixture
def opsqueue_lenient_heartbeat() -> Generator[OpsqueueProcess, None, None]:
    with opsqueue_service(
        command_args=(
            "--heartbeat-interval",
            BENCHMARK_HEARTBEAT_INTERVAL,
            "--max-missable-heartbeats",
            BENCHMARK_MAX_MISSABLE_HEARTBEATS,
        )
    ) as process:
        yield process


@dataclass(frozen=True)
class Backlog:
    """An ordered list of (company_id, chunk_count) submissions."""

    submissions: list[tuple[int, int]]

    @property
    def total_chunks(self) -> int:
        return sum(count for _company, count in self.submissions)


def make_backlog(chunks_per_company: list[int], whale_first: bool) -> Backlog:
    """Build a backlog, inserting the largest submission first (whale_first) or last."""
    companies = list(enumerate(chunks_per_company))  # (company_id, chunk_count)
    ordered = list(reversed(companies)) if whale_first else companies
    return Backlog(submissions=ordered)


def insert_backlog(
    producer_client: ProducerClient, backlog: Backlog
) -> list[SubmissionId]:
    inserted: list[SubmissionId] = []
    for company_id, amount_chunks in backlog.submissions:
        for submission in batched(range(amount_chunks), amount_chunks):
            inserted.append(
                producer_client.insert_submission(
                    submission,
                    chunk_size=1,
                    strategic_metadata={"company_id": company_id},
                )
            )
    return inserted


@dataclass
class ReservationTrace:
    """``reserve_times[i]`` is the latency of the (i+1)-th reserve; ``companies[i]`` is
    the company it came from."""

    reserve_times: list[float]
    companies: list[int]


def drive_reserve_progressively(
    consumer_client: ConsumerClient,
    strategy: object,
    total_chunks: int,
    company_of_submission: dict[int, int],
    progress_label: str = "",
) -> ReservationTrace:
    """Reserve every chunk one at a time without completing, timing each call, then
    complete them all at the end. Progress is printed every ~1% so long runs aren't
    silent."""
    reserve_times: list[float] = []
    companies: list[int] = []
    reserved: list[Chunk] = []
    step = max(1, total_chunks // 100)

    def report(phase: str, done: int) -> None:
        pct = 100 * done / total_chunks if total_chunks else 100.0
        print(
            f"[{progress_label}] {phase}: {done}/{total_chunks} ({pct:.0f}%)",
            flush=True,
        )

    for i in range(total_chunks):
        t0 = time.perf_counter()
        chunks = consumer_client.reserve_chunks(1, strategy=strategy)
        reserve_times.append(time.perf_counter() - t0)
        assert len(chunks) == 1
        # `.id` is the plain int used as the picklable key of `company_of_submission`.
        companies.append(company_of_submission[chunks[0].submission_id.id])
        reserved.append(chunks[0])
        if (i + 1) % step == 0 or i + 1 == total_chunks:
            report("reserve", i + 1)
    for j, chunk in enumerate(reserved):
        consumer_client.complete_chunk(
            submission_id=chunk.submission_id,
            submission_prefix=chunk.submission_prefix,
            chunk_index=chunk.chunk_index,
            output_content=b"DONE",
        )
        if (j + 1) % step == 0 or j + 1 == total_chunks:
            report("complete", j + 1)
    return ReservationTrace(reserve_times, companies)


def check_fair_interleaving(companies: list[int], tolerance: int, context: str) -> None:
    """Record a violation if, at any prefix, the spread between the most- and
    least-served companies that still have work outstanding exceeds ``tolerance``."""
    total_per_company = Counter(companies)
    served: Counter[int] = Counter()
    for company in companies:
        served[company] += 1
        outstanding = {
            c: served[c] for c in total_per_company if served[c] < total_per_company[c]
        }
        if len(outstanding) >= 2:
            spread = max(outstanding.values()) - min(outstanding.values())
            if spread > tolerance:
                fail(
                    f"[{context}] unfair interleaving: spread between most- and "
                    f"least-served in-progress companies was {spread} (> {tolerance})"
                )
                return


def _write_csv(trace: ReservationTrace, path: str) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["reserve_time", "company_id"])
        for t, c in zip(trace.reserve_times, trace.companies):
            writer.writerow([t, c])


def _read_csv(path: str) -> ReservationTrace:
    reserve_times: list[float] = []
    companies: list[int] = []
    with open(path, "r") as f:
        for row in csv.DictReader(f):
            reserve_times.append(float(row["reserve_time"]))
            companies.append(int(row["company_id"]))
    return ReservationTrace(reserve_times, companies)


def _consumer_worker(
    port: int,
    store_uri: str,
    total_chunks: int,
    company_of_submission: dict[int, int],
    strategy_description: StrategyDescription,
    trace_csv: str,
    progress_label: str,
) -> None:
    # Module-level with only picklable args so it survives the `forkserver` start method.
    consumer_client = ConsumerClient(f"localhost:{port}", store_uri)
    strategy = strategy_from_description(strategy_description)
    trace = drive_reserve_progressively(
        consumer_client, strategy, total_chunks, company_of_submission, progress_label
    )
    _write_csv(trace, trace_csv)


def run_cell(
    opsqueue: OpsqueueProcess,
    shape_name: str,
    chunks_per_company: list[int],
    whale_first: bool,
    strategy_label: str,
    strategy_description: StrategyDescription,
) -> ReservationTrace:
    """Insert a fresh backlog of the given shape and drive one strategy through it."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    order_label = "whale_first" if whale_first else "whale_last"
    slug = f"{shape_name}__{order_label}__{_slug(strategy_label)}"
    store_uri = (
        f"file:///tmp/opsqueue/{slug}"  # distinct store so runs never share a backlog
    )

    producer_client = ProducerClient(f"localhost:{opsqueue.port}", store_uri)
    backlog = make_backlog(chunks_per_company, whale_first=whale_first)
    inserted = insert_backlog(producer_client, backlog)
    company_of_submission = {
        submission_id.id: company
        for submission_id, (company, _count) in zip(inserted, backlog.submissions)
    }

    trace_csv = os.path.join(OUTPUT_DIR, f"{slug}.csv")
    if os.path.exists(trace_csv):
        os.remove(trace_csv)

    with background_process(
        _consumer_worker,
        args=(
            opsqueue.port,
            store_uri,
            backlog.total_chunks,
            company_of_submission,
            strategy_description,
            trace_csv,
            slug,
        ),
    ):
        for submission_id in inserted:
            producer_client.blocking_stream_completed_submission(submission_id)

    return _read_csv(trace_csv)


def _slug(text: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in text)


def _drop_outliers(xs: list[int], ys: list[float]) -> tuple[list[int], list[float]]:
    """Drop points above a Tukey upper fence, so a rare latency spike can't rescale the
    subplot and hide the curve. Plotting only; the mean still uses every sample."""
    if len(ys) < 4:
        return xs, ys
    q1, _q2, q3 = statistics.quantiles(ys, n=4)
    upper_fence = q3 + 1.5 * (q3 - q1)
    kept = [(x, y) for x, y in zip(xs, ys) if y <= upper_fence]
    if not kept:
        return xs, ys
    kept_xs, kept_ys = zip(*kept)
    return list(kept_xs), list(kept_ys)


def test_prefer_distinct_benchmark(
    opsqueue_lenient_heartbeat: OpsqueueProcess,
) -> None:
    """Sweep the (backlog shape x insert order x strategy) matrix into one figure."""
    opsqueue = opsqueue_lenient_heartbeat
    shapes = list(BACKLOG_SHAPES.items())
    insert_orders = [(True, "whale_first"), (False, "whale_last")]

    fig, axes = plt.subplots(
        len(shapes),
        len(insert_orders),
        figsize=(7 * len(insert_orders), 4.5 * len(shapes)),
        squeeze=False,
    )
    fig.suptitle(
        "PreferDistinct benchmark: reservation latency as the reserved set grows\n"
        "(rows = backlog shape, cols = insert order; lines = strategy)",
        fontsize=14,
    )

    for row, (shape_name, chunks_per_company) in enumerate(shapes):
        for col, (whale_first, order_label) in enumerate(insert_orders):
            ax = axes[row][col]
            ax.set_title(f"{shape_name}  /  {order_label}", fontsize=11)
            ax.set_xlabel("Reservations so far (reserved set size)")
            ax.set_ylabel("Time to reserve (ms)")
            ax.grid(True, linestyle=":", alpha=0.6)

            for strategy_label, strategy_description in STRATEGIES:
                trace = run_cell(
                    opsqueue,
                    shape_name,
                    chunks_per_company,
                    whale_first,
                    strategy_label,
                    strategy_description,
                )
                # Drop the first reservation (one-off warm-up cost). Times in ms.
                times = [t * 1e3 for t in trace.reserve_times[1:]]
                xs = list(range(1, 1 + len(times)))
                avg = statistics.fmean(times) if times else 0.0

                # Plot the series (outliers dropped for readability) and overlay its
                # mean (computed from every sample) as a dashed line in the same colour.
                plot_xs, plot_times = _drop_outliers(xs, times)
                (line,) = ax.plot(plot_xs, plot_times, alpha=0.6, label=strategy_label)
                ax.axhline(
                    avg,
                    color=line.get_color(),
                    linestyle="--",
                    linewidth=1.5,
                    alpha=0.9,
                )
                ax.annotate(
                    f"avg {avg:.3f}ms",
                    xy=(1.0, avg),
                    xycoords=("axes fraction", "data"),
                    xytext=(-4, 2),
                    textcoords="offset points",
                    ha="right",
                    va="bottom",
                    fontsize=8,
                    color=line.get_color(),
                )

                # Fairness applies to PreferDistinct only (Random is the unfair baseline).
                if strategy_label.startswith("PreferDistinct"):
                    check_fair_interleaving(
                        trace.companies,
                        tolerance=1,
                        context=f"{shape_name}/{order_label}",
                    )

            ax.legend(fontsize=8)

    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(COMBINED_FIGURE, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Combined benchmark figure saved to: {COMBINED_FIGURE}")

    # Non-fatal: report collected violations but exit successfully.
    print("\n" + "=" * 72)
    if _FAILURES:
        print(f"PROPERTY VIOLATIONS ({len(_FAILURES)}):")
        for i, message in enumerate(_FAILURES, 1):
            print(f"  {i}. {message}")
    else:
        print("No property violations: all fairness checks passed.")
    print("=" * 72)
