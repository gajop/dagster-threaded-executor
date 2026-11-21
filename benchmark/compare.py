import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

HERE = Path(__file__).resolve().parent
OUTPUT = HERE / "runs"
THREADED = "#1f6d88"
MULTIPROCESS = "#a75115"
ASYNC = "#8766a8"
ROWS = [
    ("one-async-sync", "Community async", ASYNC, 0),
    ("one-threaded-50", "Our threaded", THREADED, 1),
    ("one-multiprocess-50", "Stock multiprocess", MULTIPROCESS, 2),
    ("one-async-sync-10", "Community async", ASYNC, 4),
    ("one-threaded-10", "Our threaded", THREADED, 5),
    ("one-multiprocess-10", "Stock multiprocess", MULTIPROCESS, 6),
    ("five-async-sync", "Community async", ASYNC, 8),
    ("five-threaded", "Our threaded", THREADED, 9),
    ("five-multiprocess", "Stock multiprocess", MULTIPROCESS, 10),
]
GROUPS = [
    (-0.72, "1 RUN · 50 SLOTS"),
    (3.24, "1 RUN · 10 SLOTS"),
    (7.24, "5 RUNS · 10 SLOTS EACH"),
]


def plot(results):
    plt.rcParams.update({"font.family": "DejaVu Sans", "axes.spines.top": False})
    fig, (time_axis, mem_axis) = plt.subplots(1, 2, figsize=(11.8, 6.4), sharey=True)
    fig.subplots_adjust(left=0.27, right=0.95, top=0.80, bottom=0.15, wspace=0.26)
    fig.text(0.055, 0.93, "50 independent BigQuery ops", fontsize=20, weight="bold")
    fig.text(
        0.055,
        0.875,
        "Same synchronous op on all three executors · Five-second wait inside BigQuery",
        color="#475569",
        fontsize=10.5,
    )
    for axis, key, title, unit, digits in (
        (time_axis, "elapsed_s", "Elapsed time", "seconds", 1),
        (
            mem_axis,
            "peak_worker_pss_plus_swap_mib",
            "Peak process-tree memory",
            "MiB",
            0,
        ),
    ):
        end = max(result[key] for result in results) * 1.20
        axis.set_xlim(0, end)
        axis.set_ylim(10.62, -1.18)
        axis.set_title(title, loc="left", fontsize=12, weight="bold")
        axis.set_xlabel(unit)
        axis.grid(axis="x", alpha=0.2)
        axis.set_axisbelow(True)
        axis.xaxis.set_major_locator(MaxNLocator(5))
        axis.tick_params(axis="y", length=0)
        axis.axhspan(3.14, 6.59, color="#f2f5f8", zorder=0)
        for spine in ("left", "right"):
            axis.spines[spine].set_visible(False)
        for result, (_, _, color, y) in zip(results, ROWS):
            value = result[key]
            axis.barh(y, value, height=0.55, color=color)
            axis.text(
                value + end * 0.015,
                y,
                f"{value:,.{digits}f}",
                va="center",
                color="#243346",
                fontsize=9.5,
                weight="bold",
            )
    time_axis.set_yticks(
        [y for _, _, _, y in ROWS], labels=[label for _, label, _, _ in ROWS]
    )
    for y, label in GROUPS:
        time_axis.text(
            -0.68,
            y,
            label,
            transform=time_axis.get_yaxis_transform(),
            color="#334155",
            fontsize=9.5,
            weight="bold",
        )
    fig.text(
        0.055,
        0.047,
        "One local trial per case. Elapsed time includes process startup. Memory is tree PSS plus proportional swap.",
        color="#475569",
        fontsize=9,
    )
    fig.savefig(OUTPUT / "multiprocess-comparison.png", dpi=180)


if __name__ == "__main__":
    results = [
        json.loads((OUTPUT / case / "result.json").read_text()) for case, *_ in ROWS
    ]
    with (OUTPUT / "results.csv").open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=results[0], lineterminator="\n")
        writer.writeheader()
        writer.writerows(results)
    plot(results)
