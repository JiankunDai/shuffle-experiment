#!/bin/bash

# 变量设置
JAR_PATH="target/scala-2.10/spark-shuffle-experiment-1.0.0.jar"
MASTER="spark://49.52.27.113:7077"
DATA_PATH="file:///home/djk/Downloads/tpch-dbgen/gen_data/lineitem-medium.tbl"
CLASS_NAME="ShuffleExperiment"
MEMORY="1G"
TEST_DIR="logs/shuffle_file_test_$(date '+%Y%m%d_%H%M%S')"
mkdir -p $TEST_DIR

# 定义函数运行任务
run_test() {
    MANAGER=$1
    MODE=$2
    
    echo "------------------------------------------------"
    echo "Running Test: Manager=$MANAGER, Mode=$MODE"
    echo "------------------------------------------------"
    
    spark-submit \
    --class edu.ecnu.ShuffleExperiment \
    --master $MASTER \
    --deploy-mode client \
    --executor-memory 1G \
    --driver-memory 512M \
    --executor-cores 2 \
    --conf spark.executor.instances=2 \
    --conf spark.dynamicAllocation.enabled=false \
    --total-executor-cores 4 \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.shuffle.manager=${mode} \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir="${TEST_DIR}/events_${mode}_${size}" \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.shuffle.consolidateFiles=false \
    --conf spark.shuffle.sort.bypassMergeThreshold=1 \
    $JAR_PATH $DATA_PATH $MODE
}

# === 实验组 1: Hash Shuffle ===
run_test "hash" "uniform"
# run_test "hash" "longtail"
# run_test "hash" "skew"

# === 实验组 2: Sort Shuffle ===
run_test "sort" "uniform"
# run_test "sort" "longtail"
# run_test "sort" "skew"