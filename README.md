# Hadoop + Spark Taxi Benchmark

This project benchmarks a small Spark analytics job on top of an HDFS-backed Docker Compose environment. It downloads the `iampalina/nyc_taxi` dataset, stores it as Parquet, uploads it to HDFS, runs the same Spark job under several configurations, saves benchmark metrics as JSON, and generates charts from the collected results. The core idea is to compare:

- `plain` execution vs. `opt` execution
- `1` DataNode vs. `3` DataNodes

The Spark job reads a Parquet dataset from HDFS and computes taxi fare metrics grouped primarily by `passenger_count`.

## Dataset

- Dataset page: <https://huggingface.co/datasets/iampalina/nyc_taxi>

Relevant dataset characteristics used in this project:

- Format: Parquet
- Main split used here: `train`
- Columns visible in the dataset viewer include `fare_amount`, `pickup_datetime`, `pickup_longitude`, `pickup_latitude`, `dropoff_longitude`, `dropoff_latitude`, and `passenger_count`

This project downloads the dataset locally and stores it as:

- `data/dataset.parquet`

## Project Structure

- [`docker-compose.yml`](docker-compose.yml) defines the HDFS and Spark services.
- [`app`](app) contains the Spark application.
- [`tools/download_dataset.py`](tools/download_dataset.py) downloads the dataset from Hugging Face.
- [`scripts/experiment.sh`](scripts/experiment.sh) runs the benchmark scenarios.
- [`tools/plot_charts.py`](tools/plot_charts.py) generates charts from JSON result files.
- `data/results/` stores benchmark JSON outputs.
- `data/results/charts/` stores generated charts.

## Architecture

The environment contains the following services:

- `namenode`: HDFS NameNode
- `datanode`: HDFS DataNode service, scalable via Docker Compose
- `spark`: Spark master
- `spark-worker`: Spark worker
- `spark-app`: container used to run the benchmarked Spark application and to support debugging

Important detail:

- The current setup has one Spark worker.
- The experiment changes the number of HDFS DataNodes, not the number of Spark workers.

Because of that, changing from `1` to `3` DataNodes changes storage topology and HDFS behavior, but it does not increase Spark compute parallelism in the same way that adding Spark workers would.

## What The Spark Job Does

The Spark application in [`app`](app) performs the following steps:

1. Connects to the Spark master.
2. Reads the Parquet dataset from HDFS.
3. Prints the schema and sample rows.
4. Counts the rows.
5. Optionally applies a simple optimization strategy:
   - `repartition(4)`
   - `cache()`
6. Builds taxi metrics.

For the `iampalina/nyc_taxi` dataset, the primary aggregation is:

- group by `passenger_count`
- compute:
  - `trip_count`
  - `avg_fare_amount`
  - `max_fare_amount`

The application also records:

- operation execution time
- total runtime
- final memory usage
- memory usage over time

## Requirements

### System

- Docker
- Docker Compose
- Internet access for downloading the dataset and building the Python image

### Python Tools On Host

Install the helper-tool dependencies from [`requiremetns.txt`](requiremetns.txt):

```bash
python3 -m pip install -r requiremetns.txt
```

These dependencies are used for:

- dataset download
- chart generation

## Quick Start

### 1. Download the dataset

```bash
python3 tools/download_dataset.py
```

By default, the dataset is saved to:

- `data/dataset.parquet`

### 2. Build and start the cluster

```bash
docker compose up -d
```

Useful web UIs:

- HDFS NameNode UI: <http://localhost:9870>
- Spark Master UI: <http://localhost:8080>

### 3. Run all benchmark experiments

```bash
bash scripts/experiment.sh
```

This script will:

1. start the cluster with `1` DataNode
2. upload the Parquet dataset to HDFS
3. run:
   - `1_plain`
   - `1_opt`
4. save results to `data/results/results_1.json`
5. restart the cluster with `3` DataNodes
6. run:
   - `3_plain`
   - `3_opt`
7. save results to `data/results/results_3.json`
8. automatically generate charts in `data/results/charts`

After `bash scripts/experiment.sh` finishes, both benchmark JSON files and chart images are already available. In the usual workflow, you do not need to run the plotting tool separately.

### 4. Generate charts

This step is optional, because `scripts/experiment.sh` already generates charts automatically. Run it only if you want to rebuild the figures from existing JSON files.

```bash
python3 tools/plot_charts.py
```

By default, charts are saved to:

- `data/results/charts`

You can also override paths:

```bash
python3 tools/plot_charts.py --input-dir data/results --output-dir data/results/charts
```

## Running The Spark App Manually

The application container is kept alive with `sleep infinity`, so you can run Spark jobs manually inside it.

Example:

```bash
docker compose exec spark-app /spark/bin/spark-submit \
  --master spark://spark:7077 \
  /opt/spark-app/main.py \
  --experiment-key manual_run \
  --dataset-path hdfs://namenode:9000/user/hadoop/dataset.parquet \
  --spark-master spark://spark:7077
```

## Result Files

Each experiment writes benchmark metrics into JSON. Example fields:

- `ops_time`: execution time of the main analytical operations
- `total_time`: total runtime including read, setup, and overhead
- `final_memory_usage`: RSS of the Python process near the end of execution
- `memory_usage_over_time`: time series for the memory plot

## Generated Charts

The chart tool creates four figures:

### `ops_time.png`

Shows the execution time of the analytical part of the job only. This is the most useful chart for comparing the actual transformation and aggregation stage.

### `total_time.png`

Shows full end-to-end runtime, including reading the dataset, Spark overhead, and setup overhead. This is the best chart for understanding what a user actually pays for in a full run.

### `final_memory_usage.png`

Shows the final measured memory footprint of the Python process. This is not peak memory, only the value sampled near the end of execution.

### `memory_usage_series.png`

Shows memory usage over time for each experiment. This helps identify whether memory stays flat, grows gradually, or spikes during parts of the workload.

## Experimental Results

The current result files in `data/results` contain the following values:

| Experiment | Ops time, s | Total time, s | Final memory, MB |
|---|---:|---:|---:|
| `1_plain` | 34.85 | 83.01 | 38.69 |
| `1_opt` | 81.16 | 350.01 | 17.65 |
| `3_plain` | 58.61 | 111.57 | 36.90 |
| `3_opt` | 93.61 | 627.23 | 31.75 |

## Analysis

### 1. The current “optimized” mode is slower in every measured scenario

Compared with `plain` mode:

- `1_opt` is about `132.9%` slower than `1_plain` in `ops_time`
- `1_opt` is about `321.6%` slower than `1_plain` in `total_time`
- `3_opt` is about `59.7%` slower than `3_plain` in `ops_time`
- `3_opt` is about `462.2%` slower than `3_plain` in `total_time`

Likely reason:

- the job reads the dataset once and performs one main aggregation
- `repartition(4)` and `cache()` add extra work
- the cache does not have enough reuse to pay back its cost

This is an inference from the code in [`app/main.py`](app/main.py) and the measured results.

### 2. Increasing HDFS DataNodes from 1 to 3 did not improve runtime

The `plain` configuration got slower:

- `1_plain` total time: `83.01 s`
- `3_plain` total time: `111.57 s`

The `opt` configuration also got slower:

- `1_opt` total time: `350.01 s`
- `3_opt` total time: `627.23 s`

Likely reasons:

- the benchmark increases the number of HDFS DataNodes, not Spark workers
- Spark compute capacity remains effectively unchanged
- additional HDFS distribution can introduce extra coordination and I/O overhead

This is also an inference from the Compose topology and the measured numbers.

### 3. Final memory usage is not enough to claim that optimization improved memory efficiency

The optimized runs ended with a lower final memory number in some cases, but that metric is only the sampled value near the end of execution, not the peak usage. The memory time-series chart should be used together with `final_memory_usage` before drawing stronger conclusions.

### 4. The most important practical conclusion

For the current workload and topology:

- `plain` mode is the best-performing mode
- adding HDFS DataNodes alone is not a useful optimization for this benchmark
- if the goal is better Spark performance, the next variable to scale should be Spark workers or executor resources, not only HDFS DataNodes

## Suggested Next Steps

- Add more than one Spark worker and repeat the benchmark.
- Measure peak memory, not only final memory.
- Separate dataset read time from aggregation time more explicitly.
- Benchmark additional Spark settings such as partition counts and executor memory.
- Extend the analysis with more domain-specific aggregations over `pickup_datetime` and `fare_amount`.

## Notes

- The root dependency file is named [`requiremetns.txt`](requiremetns.txt), which is slightly unusual but currently used as-is by the project.
- The chart tool currently expects benchmark JSON files under `data/results`.
