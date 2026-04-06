import json
import os
import threading
import time
from pathlib import Path

import psutil
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from tap import Tap


MemoryUsagePoint = tuple[float, float]
ExperimentResults = dict[str, float | list[MemoryUsagePoint]]
ResultsByExperiment = dict[str, ExperimentResults]


class Args(Tap):
    optimize: bool = False
    experiment_key: str
    result_json_path: Path = Path(os.environ.get("RESULT_JSON_PATH", "results.json"))
    dataset_path: str = os.environ.get(
        "DATASET_PATH",
        "hdfs://localhost:9000/user/hadoop/dataset.parquet",
    )
    spark_master: str = os.environ.get("SPARK_MASTER_URL", "spark://localhost:7077")

    def configure(self) -> None:
        self.add_argument(
            "--optimize",
            action="store_true",
            help="Apply Spark optimizations (cache, repartition)",
        )
        self.add_argument(
            "--experiment-key",
            required=True,
            help="Experiment key in the format '<datanode>_<plain|opt>'",
        )
        self.add_argument(
            "--result-json-path",
            default=self.result_json_path,
            help="Path to the JSON file where experiment results will be stored",
        )
        self.add_argument(
            "--dataset-path",
            default=self.dataset_path,
            help="Path to the input dataset",
        )
        self.add_argument(
            "--spark-master",
            default=self.spark_master,
            help="Spark master URL used for the session",
        )


def memory_monitor(
    monitor_list: list[MemoryUsagePoint],
    stop_event: threading.Event,
    start_time: float,
    interval: float = 0.5,
) -> None:
    process = psutil.Process(os.getpid())
    while not stop_event.is_set():
        elapsed = time.time() - start_time
        mem_usage_mb = process.memory_info().rss / (1024 * 1024)
        monitor_list.append((elapsed, mem_usage_mb))
        time.sleep(interval)


def find_column(df: DataFrame, candidates: list[str]) -> str | None:
    columns_map = {column.lower(): column for column in df.columns}
    for candidate in candidates:
        matched = columns_map.get(candidate.lower())
        if matched:
            return matched
    return None


def build_taxi_metrics(df: DataFrame) -> DataFrame:
    pickup_datetime_col = find_column(df, ["pickup_datetime", "tpep_pickup_datetime"])
    fare_amount_col = find_column(df, ["fare_amount", "total_amount"])
    passenger_col = find_column(df, ["passenger_count"])
    distance_col = find_column(df, ["trip_distance"])

    if passenger_col and fare_amount_col:
        print(
            f"Aggregating by {passenger_col} and computing fare metrics from {fare_amount_col} "
            "for the iampalina/nyc_taxi dataset..."
        )
        return (
            df.withColumn(passenger_col, F.col(passenger_col).cast("int"))
            .withColumn(fare_amount_col, F.col(fare_amount_col).cast("double"))
            .filter(F.col(passenger_col).isNotNull())
            .filter(F.col(fare_amount_col).isNotNull())
            .filter(F.col(passenger_col) > 0)
            .groupBy(passenger_col)
            .agg(
                F.count("*").alias("trip_count"),
                F.avg(F.col(fare_amount_col)).alias("avg_fare_amount"),
                F.max(F.col(fare_amount_col)).alias("max_fare_amount"),
            )
            .orderBy(F.asc(passenger_col))
        )

    if passenger_col and distance_col:
        print(
            f"Aggregating by column {passenger_col} and computing the average distance from {distance_col}..."
        )
        return (
            df.groupBy(passenger_col)
            .agg(
                F.count("*").alias("trip_count"),
                F.avg(F.col(distance_col)).alias("avg_trip_distance"),
                F.max(F.col(distance_col)).alias("max_trip_distance"),
            )
            .orderBy(F.desc("trip_count"))
        )

    if pickup_datetime_col and fare_amount_col:
        print(
            f"Aggregating by pickup hour from {pickup_datetime_col} and computing fare metrics from "
            f"{fare_amount_col}..."
        )
        return (
            df.withColumn(pickup_datetime_col, F.to_timestamp(F.col(pickup_datetime_col)))
            .withColumn(fare_amount_col, F.col(fare_amount_col).cast("double"))
            .filter(F.col(pickup_datetime_col).isNotNull())
            .filter(F.col(fare_amount_col).isNotNull())
            .withColumn("pickup_hour", F.hour(F.col(pickup_datetime_col)))
            .groupBy("pickup_hour")
            .agg(
                F.count("*").alias("trip_count"),
                F.avg(F.col(fare_amount_col)).alias("avg_fare_amount"),
                F.max(F.col(fare_amount_col)).alias("max_fare_amount"),
            )
            .orderBy(F.asc("pickup_hour"))
        )

    fallback_col = df.columns[0]
    print(f"Taxi dataset-specific columns were not found, using fallback aggregation by {fallback_col}...")
    return (
        df.groupBy(fallback_col)
        .agg(F.count("*").alias("trip_count"))
        .orderBy(F.desc("trip_count"))
    )


def main(
    optimize: bool,
    experiment_key: str,
    result_json_path: Path,
    dataset_path: str,
    spark_master: str,
) -> None:
    try:
        result_json_path = Path(result_json_path)

        spark = (
            SparkSession.builder.appName("NYCTaxiSparkExperiment")
            .master(spark_master)
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.cores", "1")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        print("Spark session created. Optimizations enabled:", optimize)
        print("Spark master:", spark_master)

        start_time = time.time()
        memory_usage_series: list[MemoryUsagePoint] = []
        stop_event = threading.Event()
        monitor_thread = threading.Thread(
            target=memory_monitor,
            args=(memory_usage_series, stop_event, start_time),
        )
        monitor_thread.start()

        print(f"Reading data from HDFS: {dataset_path}")
        df = spark.read.parquet(dataset_path)

        print("Dataset schema:")
        df.printSchema()
        print("First rows:")
        df.show(10, truncate=False)

        count_initial = df.count()
        print(f"Number of rows in the dataset: {count_initial}")

        if optimize:
            print("Applying optimizations: repartition and cache")
            df = df.repartition(4)
            df.cache()
            df.count()

        t_opt_start = time.time()

        metrics_df = build_taxi_metrics(df)
        preview_results = metrics_df.collect()
        print("Aggregation results:")
        print(preview_results[:5])

        t1 = time.time()
        elapsed_operations = t1 - t_opt_start
        elapsed_total = t1 - start_time
        print(f"Total execution time: {elapsed_total:.2f} seconds")
        print(f"Execution time for operations after optimization: {elapsed_operations:.2f} seconds")

        stop_event.set()
        monitor_thread.join()

        final_mem_usage = memory_usage_series[-1][1] if memory_usage_series else 0.0
        print("Final memory usage: {:.2f} MB".format(final_mem_usage))

        results_data: ExperimentResults = {
            "ops_time": elapsed_operations,
            "total_time": elapsed_total,
            "final_memory_usage": final_mem_usage,
            "memory_usage_over_time": memory_usage_series,
        }

        if result_json_path.exists():
            with result_json_path.open("r", encoding="utf-8") as json_file:
                all_results: ResultsByExperiment = json.load(json_file)
        else:
            all_results = {}

        all_results[experiment_key] = results_data

        with result_json_path.open("w", encoding="utf-8") as json_file:
            json.dump(all_results, json_file, indent=4)
        print(f"Experiment results for '{experiment_key}' were saved to {result_json_path}")

        spark.stop()
    except Exception as error:
        print("An error occurred while running the Spark application:")
        print(error)
        raise SystemExit(1)


if __name__ == "__main__":
    args = Args().parse_args()
    main(
        args.optimize,
        args.experiment_key,
        args.result_json_path,
        args.dataset_path,
        args.spark_master,
    )
