import json
from pathlib import Path

from tap import Tap

BAR_COLORS = {
    "ops_time": "#4C78A8",
    "total_time": "#F58518",
    "final_memory_usage": "#54A24B",
    "peak_memory_usage": "#E45756",
}

LINE_COLORS = ["#4C78A8", "#F58518", "#E45756", "#72B7B2", "#B279A2", "#FF9DA6"]
PHASE_COLORS = {
    "spark_session_time": "#4C78A8",
    "read_time": "#72B7B2",
    "inspection_time": "#F58518",
    "initial_scan_time": "#ECA82C",
    "optimization_time": "#E45756",
    "query_time": "#54A24B",
}
PHASE_LABELS = {
    "spark_session_time": "Spark session",
    "read_time": "Read plan",
    "inspection_time": "Inspect",
    "initial_scan_time": "Initial scan",
    "optimization_time": "Optimization prep",
    "query_time": "Query",
}


class Args(Tap):
    input_dir: Path = Path("data/results")
    output_dir: Path = Path("data/results/charts")


def configure_plot_style() -> None:
    import matplotlib.pyplot as plt

    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update(
        {
            "figure.facecolor": "#F8F7F4",
            "axes.facecolor": "#FFFFFF",
            "axes.edgecolor": "#D9D4CC",
            "axes.labelcolor": "#2F2A24",
            "axes.titleweight": "semibold",
            "axes.titlesize": 16,
            "axes.labelsize": 11,
            "xtick.color": "#4A433B",
            "ytick.color": "#4A433B",
            "grid.color": "#E8E1D8",
            "grid.linewidth": 0.8,
            "grid.alpha": 0.8,
            "legend.frameon": False,
            "font.family": "DejaVu Sans",
        }
    )


def discover_result_files(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("results*.json"))


def load_results(files: list[Path]) -> dict[str, dict]:
    data: dict[str, dict] = {}

    for file_path in files:
        with file_path.open("r", encoding="utf-8") as file:
            data.update(json.load(file))

    return data


def save_figure(output_dir: Path, filename: str, dpi: int = 300) -> None:
    import matplotlib.pyplot as plt

    output_dir.mkdir(parents=True, exist_ok=True)
    save_path = output_dir / filename
    plt.tight_layout()
    plt.savefig(save_path, dpi=dpi, bbox_inches="tight")
    plt.close()
    print(f"Saved plot: {save_path}")


def create_bar_plot(
    experiment_keys: list[str],
    values: list[float],
    ylabel: str,
    title: str,
    filename: str,
    color: str,
    output_dir: Path,
    dpi: int = 300,
) -> None:
    import matplotlib.pyplot as plt

    fig, axis = plt.subplots(figsize=(9, 6))
    bars = axis.bar(experiment_keys, values, color=color, width=0.62, edgecolor="#FFFFFF")

    axis.set_xlabel("Experiment")
    axis.set_ylabel(ylabel)
    axis.set_title(title, pad=14)

    for bar, value in zip(bars, values):
        axis.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:.2f}",
            ha="center",
            va="bottom",
            fontsize=10,
            color="#2F2A24",
        )

    save_figure(output_dir, filename, dpi)


def create_memory_series_plot(
    experiment_keys: list[str],
    data: dict[str, dict],
    output_dir: Path,
    dpi: int = 300,
) -> None:
    import matplotlib.pyplot as plt

    fig, axis = plt.subplots(figsize=(10, 6.5))

    for index, key in enumerate(experiment_keys):
        series = data[key].get("memory_usage_over_time", [])
        if not series:
            print(f"No memory usage data for experiment {key}")
            continue

        times, memories = zip(*series)
        axis.plot(
            times,
            memories,
            label=key,
            linewidth=2.4,
            color=LINE_COLORS[index % len(LINE_COLORS)],
        )

    axis.set_xlabel("Time (seconds)")
    axis.set_ylabel("Memory usage (MB)")
    axis.set_title("Memory usage over time", pad=14)
    axis.legend(title="Experiment")

    save_figure(output_dir, "memory_usage_series.png", dpi)


def create_stacked_phase_plot(
    experiment_keys: list[str],
    data: dict[str, dict],
    output_dir: Path,
    dpi: int = 300,
) -> None:
    import matplotlib.pyplot as plt

    fig, axis = plt.subplots(figsize=(11, 6.5))
    bottom = [0.0] * len(experiment_keys)

    for phase_key in PHASE_LABELS:
        values = [float(data[key].get(phase_key, 0.0)) for key in experiment_keys]
        axis.bar(
            experiment_keys,
            values,
            bottom=bottom,
            label=PHASE_LABELS[phase_key],
            color=PHASE_COLORS[phase_key],
            width=0.68,
            edgecolor="#FFFFFF",
        )
        bottom = [current + value for current, value in zip(bottom, values)]

    for index, total in enumerate(bottom):
        axis.text(
            index,
            total,
            f"{total:.2f}",
            ha="center",
            va="bottom",
            fontsize=10,
            color="#2F2A24",
        )

    axis.set_xlabel("Experiment")
    axis.set_ylabel("Time (seconds)")
    axis.set_title("Runtime breakdown by execution phase", pad=14)
    axis.legend(title="Phase", ncols=2)

    save_figure(output_dir, "runtime_breakdown.png", dpi)


def create_phase_share_plot(
    experiment_keys: list[str],
    data: dict[str, dict],
    output_dir: Path,
    dpi: int = 300,
) -> None:
    import matplotlib.pyplot as plt

    fig, axis = plt.subplots(figsize=(11, 6.5))
    bottom = [0.0] * len(experiment_keys)

    for phase_key in PHASE_LABELS:
        raw_values = [float(data[key].get(phase_key, 0.0)) for key in experiment_keys]
        shares = []
        for experiment_key, raw_value in zip(experiment_keys, raw_values):
            total = float(data[experiment_key].get("total_time", 0.0))
            shares.append((raw_value / total * 100.0) if total else 0.0)

        axis.bar(
            experiment_keys,
            shares,
            bottom=bottom,
            label=PHASE_LABELS[phase_key],
            color=PHASE_COLORS[phase_key],
            width=0.68,
            edgecolor="#FFFFFF",
        )
        bottom = [current + value for current, value in zip(bottom, shares)]

    axis.set_xlabel("Experiment")
    axis.set_ylabel("Share of total runtime (%)")
    axis.set_title("Runtime phase share by experiment", pad=14)
    axis.set_ylim(0, 100)
    axis.legend(title="Phase", ncols=2)

    save_figure(output_dir, "runtime_phase_share.png", dpi)


def main() -> None:
    args = Args(underscores_to_dashes=True).parse_args()
    configure_plot_style()

    result_files = discover_result_files(args.input_dir)
    if not result_files:
        raise FileNotFoundError(f"No result files matching 'results*.json' were found in {args.input_dir}")

    data = load_results(result_files)
    experiment_keys = sorted(data.keys())
    print(f"Experiments: {experiment_keys}")

    ops_times = [data[key]["ops_time"] for key in experiment_keys]
    total_times = [data[key]["total_time"] for key in experiment_keys]
    final_memory_usages = [data[key]["final_memory_usage"] for key in experiment_keys]
    peak_memory_usages = [
        float(data[key].get("peak_memory_usage", data[key]["final_memory_usage"]))
        for key in experiment_keys
    ]

    create_bar_plot(
        experiment_keys=experiment_keys,
        values=ops_times,
        ylabel="Execution time (seconds)",
        title="Operation execution time by experiment",
        filename="ops_time.png",
        color=BAR_COLORS["ops_time"],
        output_dir=args.output_dir,
    )
    create_bar_plot(
        experiment_keys=experiment_keys,
        values=total_times,
        ylabel="Total time (seconds)",
        title="Total runtime by experiment",
        filename="total_time.png",
        color=BAR_COLORS["total_time"],
        output_dir=args.output_dir,
    )
    create_bar_plot(
        experiment_keys=experiment_keys,
        values=final_memory_usages,
        ylabel="Memory usage (MB)",
        title="Final driver RSS memory usage by experiment",
        filename="final_memory_usage.png",
        color=BAR_COLORS["final_memory_usage"],
        output_dir=args.output_dir,
    )
    create_bar_plot(
        experiment_keys=experiment_keys,
        values=peak_memory_usages,
        ylabel="Memory usage (MB)",
        title="Peak driver RSS memory usage by experiment",
        filename="peak_memory_usage.png",
        color=BAR_COLORS["peak_memory_usage"],
        output_dir=args.output_dir,
    )
    create_memory_series_plot(
        experiment_keys=experiment_keys,
        data=data,
        output_dir=args.output_dir,
    )
    create_stacked_phase_plot(
        experiment_keys=experiment_keys,
        data=data,
        output_dir=args.output_dir,
    )
    create_phase_share_plot(
        experiment_keys=experiment_keys,
        data=data,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
