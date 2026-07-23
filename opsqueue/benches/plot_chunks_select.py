#!/usr/bin/env python3
"""Plot the chunks_select benchmark: one subplot per backlog shape, a line per strategy.

Reads target/chunks_select_bench.csv (written by the bench) and writes a PNG next to
this script so it can be committed alongside the code it measures.

"""

import csv
import matplotlib.pyplot as plt  # noqa: E402
from collections import defaultdict

CSV_PATH = "opsqueue/chunks_select_bench.csv"
OUT_PATH = "chunks_select_bench.svg"


def main() -> None:
    # (shape, strategy) -> {backlog_size: (p10_us, median_us, p90_us)}
    series = defaultdict(lambda: defaultdict(dict))
    shapes: list[str] = []
    max_x = 0

    print(f"Reading CSV: {CSV_PATH}")
    with open(CSV_PATH, newline="") as f:
        for row in csv.DictReader(f):
            shape = row["shape"]
            size = int(row["backlog_size"])
            max_x = max(max_x, size)
            series[shape][row["strategy"]][size] = (
                float(row["p10_us"]),
                float(row["median_us"]),
                float(row["p90_us"]),
            )
            if shape not in shapes:
                shapes.append(shape)

    fig, axes = plt.subplots(
        1, len(shapes), figsize=(7.5 * len(shapes), 5.5), squeeze=False
    )
    fig.suptitle("Reservation duration (no network in path)")

    for ax, shape in zip(axes.flatten(), shapes):
        for strategy, points in sorted(series[shape].items()):
            xs = sorted(points)
            p10s, medians, p90s = zip(*[points[x] for x in xs])
            (line,) = ax.plot(xs, medians, marker="o", label=strategy)
            ax.fill_between(xs, p10s, p90s, color=line.get_color(), alpha=0.2)

        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlim(right=max_x)
        ax.set_ylim(top=1_000_000)
        ax.set_title(shape)
        ax.set_xlabel("Backlog size (total chunks)")
        ax.set_ylabel("Latency to select first chunk (us) [Median ± P10/P90]")
        ax.grid(True, which="both", linestyle=":", alpha=0.5)
        ax.legend(fontsize=9)

    fig.tight_layout()
    fig.savefig(OUT_PATH, dpi=150, bbox_inches="tight")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
