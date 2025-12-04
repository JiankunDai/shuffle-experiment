#!/bin/bash
# run_skew_experiment.sh - 数据倾斜(Skew)对Hash/Sort Shuffle性能影响的全量对比测试

# ================= 配置区域 =================
JAR_PATH="target/scala-2.10/spark-shuffle-experiment-1.0.0.jar"
MASTER="spark://49.52.27.113:7077"  # 请确保Master地址正确
# 内存配置：为了让Skew=2.0有机会跑完，建议给到2G或4G；若要测试OOM极限可调小
EXECUTOR_MEM="2G" 
TEST_DIR="logs/skew_test_$(date '+%Y%m%d_%H%M%S')"
RESULT_CSV="${TEST_DIR}/skew_comparison_results.csv"

mkdir -p $TEST_DIR

echo "=========================================================="
echo "   Spark Shuffle 数据倾斜性能对比实验 (Hash vs Sort)   "
echo "=========================================================="
echo "日志目录: $TEST_DIR"
echo "结果汇总: $RESULT_CSV"
echo "配置内存: Executor=${EXECUTOR_MEM}"
echo ""

# 初始化CSV表头
echo "Shuffle Manager,Dataset Size,Zipf Skew,Time (ms),Shuffle Size,Status" > $RESULT_CSV

# ================= 核心测试函数 =================
run_test() {
    local mode=$1      # hash / sort
    local size=$2      # small / large
    local skew=$3      # 0.0 / 1.0 / 2.0
    
    local log_file="${TEST_DIR}/${mode}_${size}_skew${skew}.log"
    
    echo ">>> [正在运行] 模式: $mode | 数据: $size | 倾斜度: $skew"
    
    # 1. 环境清理 (防止临时文件干扰)
    rm -rf /tmp/spark/work/*
    
    # 2. 提交任务
    # 注意：参数顺序必须与 Scala 代码 main 函数一致：args(0)=mode, args(1)=size, args(2)=skew
    spark-submit \
        --class edu.ecnu.ShuffleExperiment \
        --master $MASTER \
        --deploy-mode client \
        --executor-memory $EXECUTOR_MEM \
        --driver-memory 1G \
        --executor-cores 2 \
        --conf spark.executor.instances=4 \
        --conf spark.default.parallelism=200 \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.shuffle.manager=${mode} \
        --conf spark.shuffle.service.enabled=false \
        --conf spark.shuffle.consolidateFiles=true \
        --conf spark.shuffle.sort.bypassMergeThreshold=200 \
        --conf spark.shuffle.compress=true \
        --conf spark.shuffle.spill.compress=true \
        $JAR_PATH \
        "$mode" "$size" "$skew" > "$log_file" 2>&1
        
    local ret_code=$?
    
    # 3. 结果解析
    local status="Success"
    local time_ms="N/A"
    local shuffle_size="N/A"
    
    if [ $ret_code -ne 0 ]; then
        status="Failed"
        echo "   ❌ 任务失败 (查看日志: $log_file)"
    else
        # 从日志中提取执行时间和数据量
        # 匹配日志行示例: "结果 [hash] - 耗时: 52751ms, Shuffle数据量: 622.24 MB"
        time_ms=$(grep "耗时:" "$log_file" | awk -F'耗时: ' '{print $2}' | awk -F'ms' '{print $1}' | tail -n 1)
        shuffle_size=$(grep "Shuffle数据量:" "$log_file" | awk -F'Shuffle数据量: ' '{print $2}' | tail -n 1)
        
        if [ -z "$time_ms" ]; then
            status="Success (No Data)" # 任务成功但没打印出结果行
        else
            echo "   ✅ 成功 - 耗时: ${time_ms}ms, 数据量: ${shuffle_size}"
        fi
    fi
    
    # 写入CSV
    echo "${mode},${size},${skew},${time_ms},${shuffle_size},${status}" >> $RESULT_CSV
    
    # 等待集群资源回收
    sleep 5
}

# ================= 执行实验矩阵 =================

# 1. 定义实验参数范围
MODES=("hash" "sort")
SIZES=("small" "large")
SKEWS=("0.0" "1.0" "2.0")

# 2. 嵌套循环执行所有组合 (共 2*2*3 = 12 次实验)
for size in "${SIZES[@]}"; do
    for skew in "${SKEWS[@]}"; do
        for mode in "${MODES[@]}"; do
            run_test "$mode" "$size" "$skew"
        done
    done
done

echo ""
echo "=========================================================="
echo "所有实验结束！"
echo "请查看汇总报告: $RESULT_CSV"
echo "=========================================================="
# 打印 CSV 内容预览
cat $RESULT_CSV | column -t -s,