package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.scheduler._

class ShuffleMetricsListener extends SparkListener {
  private var shuffleWriteBytes: Long = 0L
  private var shuffleReadBytes: Long = 0L
  
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    if (metrics != null) {
      // 处理 shuffle 写指标
      metrics.shuffleWriteMetrics.foreach { writeMetrics =>
        shuffleWriteBytes += writeMetrics.shuffleBytesWritten
      }
      
      // 处理 shuffle 读指标
      metrics.shuffleReadMetrics.foreach { readMetrics =>
        shuffleReadBytes += readMetrics.remoteBytesRead
      }
    }
  }
  
  def getShuffleWriteBytes: Long = shuffleWriteBytes
  def reset(): Unit = shuffleWriteBytes = 0L
}

object ShuffleExperiment {
  
  def runHashShuffleExperiment(df: DataFrame, sc: SparkContext): (Long, Long) = {
    val listener = new ShuffleMetricsListener
    sc.addSparkListener(listener)
    
    val startTime = System.currentTimeMillis()
    
    // Hash Shuffle 操作
    val result = df.rdd
      .map(row => ((row.getString(2), row.getString(5)), row.getDouble(4))) // (category, region) -> value
      .reduceByKey(_ + _)
      .count()
    
    val endTime = System.currentTimeMillis()
    val shuffleBytes = listener.getShuffleWriteBytes

    listener.reset()
    (endTime - startTime, shuffleBytes)
  }
  
  def runSortShuffleExperiment(df: DataFrame, sc: SparkContext): (Long, Long) = {
    val listener = new ShuffleMetricsListener
    sc.addSparkListener(listener)
    
    val startTime = System.currentTimeMillis()
    
    // Sort Shuffle 操作
    val result = df.sort("userId", "timestamp")
      .select("userId", "itemId", "value")
      .count()
    
    val endTime = System.currentTimeMillis()
    val shuffleBytes = listener.getShuffleWriteBytes
    
    listener.reset()
    
    (endTime - startTime, shuffleBytes)
  }

  def formatBytes(bytes: Long): String = {
    val units = Array("B", "KB", "MB", "GB", "TB")
    if (bytes <= 0) return "0 B"
    
    val digitGroups = (Math.log10(bytes.toDouble) / Math.log10(1024)).toInt
    val unit = units(Math.min(digitGroups, units.length - 1))
    val value = bytes / Math.pow(1024, Math.min(digitGroups, units.length - 1))
    
    f"$value%.2f $unit"
  }

  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark1.6-Shuffle-Experiment")
      // .setMaster("local[4]")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.shuffle.manager", "hash") // 或 "sort"
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    println("=== 集群资源信息 ===")
    println(s"Application ID: ${sc.applicationId}")
    println(s"Executor 数量: ${sc.getExecutorMemoryStatus.size}")
    println(s"默认并行度: ${sc.defaultParallelism}")
    
    sc.getExecutorMemoryStatus.foreach { case (executorId, (memory, _)) =>
      println(s"Executor $executorId: 内存 = $memory")
    }

    sc.setLogLevel("ALL")
    
    try {
      val datasets = DataGenerator.generateDatasets(sqlContext)
      
      for ((size, df) <- datasets) {
        println(s"\n=== 测试数据集大小: $size ===")
        println(s"数据量: ${df.count()} 条记录")

        println(s"存储级别: ${df.rdd.getStorageLevel}")

        println(s"分区数: ${df.rdd.partitions.length}")

        val sample = df.rdd.mapPartitions(iter => Iterator(iter.next())).collect()
        println(s"样本数据: ${sample.take(1).mkString(", ")}")
        
        // 测试 Hash Shuffle
        sc.getConf.set("spark.shuffle.manager", "hash")
        println("执行 Hash Shuffle...")
        val (hashTime, hashBytes) = runHashShuffleExperiment(df, sc)
        
        // 测试 Sort Shuffle  
        sc.getConf.set("spark.shuffle.manager", "sort")
        println("执行 Sort Shuffle...")
        val (sortTime, sortBytes) = runSortShuffleExperiment(df, sc)
        
        println(s"Hash Shuffle - 时间: ${hashTime}ms, Shuffle数据: ${formatBytes(hashBytes)} bytes")
        println(s"Sort Shuffle - 时间: ${sortTime}ms, Shuffle数据: ${formatBytes(sortBytes)} bytes")
        
        // 显示一些统计信息
        println(s"数据集统计: 分区数 = ${df.rdd.partitions.length}")
      }
      
    } finally {
      sc.stop()
    }
  }
}