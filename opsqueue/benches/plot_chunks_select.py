#!/usr/bin/env python3
"""Plot the chunks_select benchmark: one subplot per backlog shape, a line per strategy.

Reads target/chunks_select_bench.csv (written by the bench) and writes a PNG next to
this script so it can be committed alongside the code it measures.
"""

import csv
import os
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

CSV_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "target", "chunks_select_bench.csv"
)
OUT_PATH = os.path.join(os.path.dirname(__file__), "chunks_select_bench.png")


def main() -> None:
    # (shape, strategy) -> {backlog_size: median_us}
    series: dict[tuple[str, str], dict[int, float]] = defaultdict(dict)
    shapes: list[str] = []
    with open(CSV_PATH, newline="") as f:
        for row in csv.DictReader(f):
            shape = row["shape"]
            series[(shape, row["strategy"])][int(row["backlog_size"])] = float(
                row["median_us"]
            )
            if shape not in shapes:
                shapes.append(shape)

    fig, axes = plt.subplots(
        1, len(shapes), figsize=(7.5 * len(shapes), 5.5), squeeze=False
    )
    fig.suptitle("PreferDistinct selection-query cost (no network in path)")
    for ax, shape in zip(axes[0], shapes):
        for (s, strategy), points in sorted(series.items()):
            if s != shape:
                continue
            xs = sorted(points)
            ax.plot(xs, [points[x] for x in xs], marker="o", label=strategy)
        ax.set_title(shape)
        ax.set_xlabel("Backlog size (total chunks)")
        ax.set_ylabel("Median time to select first chunk (us)")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.grid(True, which="both", linestyle=":", alpha=0.5)
        ax.legend(fontsize=9)

    fig.tight_layout()
    fig.savefig(OUT_PATH, dpi=150, bbox_inches="tight")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
