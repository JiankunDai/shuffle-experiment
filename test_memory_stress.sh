#!/bin/bash
# test_memory_stress.sh - 内存压力与溢写机制测试

# 接逼出 Sort Shuffle 的“磁盘溢写（Spill）”特性以及 Hash Shuffle 的“内存溢出（OOM）”极限。
# 脚本设计思路
# 这个脚本基于您之前的 test_file_monitor.sh 进行了以下关键升级：

# 新增内存循环：遍历 512M, 1G, 2G, 4G 四种内存配置。

# 状态检测：增加了对 Spark 任务成功/失败的捕获（因为 Hash Shuffle 极大概率会挂掉，我们需要记录下“Failure/OOM”而不是空数据）。

# CSV 增强：结果表格增加 Memory 和 Status 列，方便画图分析“内存-性能”拐点。

set +e  # 关闭错误立即退出，保证即使OOM也能继续跑下一个测试

JAR_PATH="target/scala-2.10/spark-shuffle-experiment-1.0.0.jar"
MASTER="spark://49.52.27.113:7077"  # 请确认您的Master地址
TEST_DIR="logs/memory_stress_test_$(date '+%Y%m%d_%H%M%S')"
mkdir -p $TEST_DIR

echo "=== Shuffle 内存限制与压力测试 ==="
echo "测试目录: $TEST_DIR"
echo "测试目标: 验证 Sort Spill 机制 vs Hash OOM 边界"
echo ""

# 结果提取函数 (增加了状态判断)
analyze_log() {
    local log_file=$1
    local mode=$2
    local size=$3
    local mem=$4
    local exit_code=$5

    # 提取耗时 (如果任务失败，可能没有耗时)
    local duration=$(grep "执行耗时:" "$log_file" | awk -F': ' '{print $2}' | sed 's/ms//')
    
    # 提取 Shuffle 数据量
    local shuffle_bytes=$(grep "Shuffle数据量:" "$log_file" | awk -F': ' '{print $2}')
    
    # 提取峰值文件数
    local file_count=$(grep "峰值Shuffle文件数:" "$log_file" | awk -F': ' '{print $2}' | sed 's/ 个//')
    
    # 判断状态
    local status="Success"
    if [ $exit_code -ne 0 ]; then
        status="Failed"
        # 尝试检测是否是 OOM
        if grep -q "OutOfMemoryError" "$log_file"; then
            status="Failed (OOM)"
        elif grep -q "ExecutorLostFailure" "$log_file"; then
            status="Failed (Executor Lost)"
        fi
        duration="N/A" # 失败没耗时
    fi

    echo "  > 配置: $mode | $size | $mem"
    echo "  > 状态: $status"
    echo "  > 耗时: ${duration}ms"
    echo ""
    
    # CSV格式: 模式,数据集,内存,状态,耗时,文件数,数据量
    echo "$mode,$size,$mem,$status,${duration},${file_count},${shuffle_bytes}"
}

# 执行核心函数
run_stress_test() {
    local mode=$1
    local size=$2
    local mem=$3
    
    local description="$mode Shuffle (Mem=$mem, Data=$size)"
    local log_file="${TEST_DIR}/stress_${mode}_${size}_${mem}.log"
    
    echo ">>> [准备] 清理环境 (rm -rf /tmp/spark/work/*) ..."
    # 这一步很关键，防止磁盘满导致假失败
    rm -rf /tmp/spark/work/*
    sleep 3
    
    echo "--- 开始测试: ${description} ---"
    
    # 注意：这里动态调整了 --executor-memory
    # --driver-memory 保持 1G 即可，主要是限制 executor
    spark-submit \
        --class edu.ecnu.ShuffleExperiment \
        --master $MASTER \
        --deploy-mode client \
        --executor-memory $mem \
        --driver-memory 1G \
        --executor-cores 2 \
        --conf spark.executor.instances=4 \
        --conf spark.dynamicAllocation.enabled=false \
        --total-executor-cores 8 \
        --conf spark.sql.adaptive.enabled=false \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.shuffle.manager=${mode} \
        --conf spark.shuffle.service.enabled=false \
        --conf spark.shuffle.consolidateFiles=true \
        --conf spark.shuffle.sort.bypassMergeThreshold=200 \
        --conf spark.shuffle.compress=true \
        --conf spark.shuffle.spill.compress=true \
        $JAR_PATH \
        "$mode" "$size" > "$log_file" 2>&1
    
    # 捕获 spark-submit 的退出码
    local ret_code=$?
    
    if [ $ret_code -eq 0 ]; then
        echo "✅ 任务成功"
    else
        echo "❌ 任务失败 (Exit Code: $ret_code) - 预期内的内存不足？"
    fi
    echo "日志: $log_file"
    echo ""
    
    # 休息一下，等待集群资源回收
    sleep 10
    
    # 立即分析并追加到结果文件
    analyze_log "$log_file" "$mode" "$size" "$mem" $ret_code >> "${TEST_DIR}/memory_stress_results.csv"
}

# === 初始化汇总文件 ===
echo "Shuffle Manager,Dataset,Memory,Status,Time(ms),File Count,Shuffle Size" > "${TEST_DIR}/memory_stress_results.csv"

# === 实验循环 ===
# 建议只跑 large 数据集，因为小数据集在 512M 内存下也能轻松跑过，看不出差异
DATA_SIZE="large" 

# 1. 内存配置列表
MEM_CONFIGS=("512M" "1G" "2G" "4G")

for mem in "${MEM_CONFIGS[@]}"; do
    # 2. 对比两种模式
    for mode in "hash" "sort"; do
        run_stress_test "$mode" "$DATA_SIZE" "$mem"
    done
done

echo "=== 测试全部完成 ==="
echo "结果汇总: ${TEST_DIR}/memory_stress_results.csv"