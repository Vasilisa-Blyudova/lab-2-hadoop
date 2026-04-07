#!/bin/bash
set -euo pipefail

readonly DATASET_LOCAL_PATH="./data/dataset.parquet"
readonly DATASET_HDFS_PATH="/user/hadoop/dataset.parquet"
readonly RESULTS_PATH_IN_CONTAINER="/opt/spark-app/results.json"
readonly RESULTS_PATH_ON_HOST="./app/results.json"
readonly RESULTS_DIR="./data/results"


check_prerequisites() {
  if [ ! -f "${DATASET_LOCAL_PATH}" ]; then
    echo "Dataset not found at ${DATASET_LOCAL_PATH}"
    exit 1
  fi

  mkdir -p "${RESULTS_DIR}"
}

wait_for_namenode() {
  echo "Waiting for NameNode to become available..."
  until docker exec namenode hdfs dfsadmin -report >/dev/null 2>&1; do
    sleep 5
  done
}

check_safemode() {
  echo "Checking HDFS safe mode..."
  while true; do
    mode="$(docker exec namenode hdfs dfsadmin -safemode get)"
    if echo "${mode}" | grep -q "Safe mode is OFF"; then
      echo "HDFS safe mode is OFF."
      break
    fi

    echo "HDFS safe mode is still ON, waiting 5 seconds..."
    sleep 5
  done
}

upload_dataset() {
  echo "Uploading dataset to HDFS..."
  docker exec namenode hdfs dfs -mkdir -p /user/hadoop
  docker exec namenode hdfs dfs -put -f /hadoop/dataset.parquet "${DATASET_HDFS_PATH}"
}

wait_for_dataset() {
  echo "Waiting for dataset to appear in HDFS..."
  until docker exec namenode hdfs dfs -test -e "${DATASET_HDFS_PATH}"; do
    sleep 5
  done
}

reset_results() {
  rm -f "${RESULTS_PATH_ON_HOST}"
}

run_spark_job() {
  local mode="${1}"
  local nodes="${2}"
  local experiment_key="${nodes}_${mode}"
  local -a cmd=(
    /spark/bin/spark-submit
    --master
    spark://spark:7077
    /opt/spark-app/main.py
    --experiment-key
    "${experiment_key}"
    --result-json-path
    "${RESULTS_PATH_IN_CONTAINER}"
    --dataset-path
    "hdfs://namenode:9000${DATASET_HDFS_PATH}"
    --spark-master
    "spark://spark:7077"
  )

  if [ "${mode}" = "opt" ]; then
    cmd+=(--optimize)
  fi

  echo "Running Spark experiment: ${experiment_key}"
  docker exec spark-app "${cmd[@]}"
}

save_results_snapshot() {
  local nodes="${1}"
  local snapshot_path="${RESULTS_DIR}/results_${nodes}.json"

  if [ ! -f "${RESULTS_PATH_ON_HOST}" ]; then
    echo "Results file ${RESULTS_PATH_ON_HOST} was not created"
    exit 1
  fi

  mv "${RESULTS_PATH_ON_HOST}" "${snapshot_path}"
  echo "Saved results snapshot to ${snapshot_path}"
}

start_cluster() {
  local datanode_count="${1}"

  echo "Building spark-app image..."
  docker compose build spark-app

  echo "Starting cluster with ${datanode_count} DataNode(s)..."
  docker compose up -d --scale datanode="${datanode_count}" namenode datanode spark spark-worker spark-app

  wait_for_namenode
  check_safemode
  upload_dataset
  wait_for_dataset
}

stop_cluster() {
  echo "Stopping cluster..."
  docker compose down -v
}

generate_charts() {
  echo "Generating charts from benchmark results..."
  python3 tools/plot_charts.py \
    --input-dir "${RESULTS_DIR}" \
    --output-dir "${RESULTS_DIR}/charts"
}

run_experiment_group() {
  local datanode_count="${1}"

  echo "==================================================="
  echo "Preparing experiments for ${datanode_count} DataNode(s)..."
  reset_results
  start_cluster "${datanode_count}"

  echo "---------------------------------------------------"
  echo "Experiment: ${datanode_count} DataNode(s), Spark Unoptimized"
  run_spark_job "plain" "${datanode_count}"

  echo "---------------------------------------------------"
  echo "Experiment: ${datanode_count} DataNode(s), Spark Optimized"
  run_spark_job "opt" "${datanode_count}"

  save_results_snapshot "${datanode_count}"
  stop_cluster
}

main() {
  check_prerequisites
  run_experiment_group 1
  run_experiment_group 3
  generate_charts

  echo "==================================================="
  echo "All experiments are complete."
}

main "$@"
