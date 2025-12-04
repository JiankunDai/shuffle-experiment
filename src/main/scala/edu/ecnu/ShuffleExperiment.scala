package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkContext, SparkConf, SparkEnv}
import org.apache.spark.scheduler._
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Success, Failure}
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID

class ShuffleFileMonitor(sparkLocalDir: String = "/tmp/spark/work") {
  private val executorService = Executors.newSingleThreadExecutor()
  private var isMonitoring = false
  private val maxFileCount = new AtomicInteger(0)
  private var maxTotalSize: Long = 0L
  
  def startMonitoring(intervalSeconds: Int = 1, durationSeconds: Int = 300): Unit = {
    isMonitoring = true
    val startTime = System.currentTimeMillis()
    
    executorService.submit(new Runnable {
      override def run(): Unit = {
        while (isMonitoring && 
               System.currentTimeMillis() - startTime < durationSeconds * 1000) {
          
          try {
            // 获取当前时间戳
            val timestamp = new java.text.SimpleDateFormat("HH:mm:ss")
              .format(new java.util.Date())
            
            // 查找shuffle文件
            val shuffleDir = new File(sparkLocalDir)
            val (fileCount, totalSizeKB) = if (shuffleDir.exists() && shuffleDir.isDirectory) {
              countShuffleFiles(shuffleDir)
            } else {
              (0, 0L)
            }
            
            // 更新最大值
            if (fileCount > maxFileCount.get()) {
              maxFileCount.set(fileCount)
            }
            if (totalSizeKB > maxTotalSize) {
              maxTotalSize = totalSizeKB
            }
            
            // 输出监控信息
            println(s"[ShuffleMonitor $timestamp] 文件数: $fileCount, " +
                   s"总大小: ${formatSizeKB(totalSizeKB)} (峰值: ${maxFileCount.get()}文件, ${formatSizeKB(maxTotalSize)})")
            
            // 如果发现文件数量异常多，可能是Hash Shuffle的特征
            if (fileCount > 1000) {
              println(s"[ShuffleMonitor 警告] 检测到大量临时文件($fileCount 个)，这可能是Hash Shuffle的特征")
            }
            
            // 等待指定间隔
            Thread.sleep(intervalSeconds * 1000)
          } catch {
            case e: Exception =>
              println(s"[ShuffleMonitor 错误] ${e.getMessage}")
          }
        }
      }
    })
  }
  
  private def countShuffleFiles(directory: File): (Int, Long) = {
    var fileCount = 0
    var totalSizeBytes: Long = 0L
    
    def scanDir(dir: File): Unit = {
      if (dir.exists() && dir.isDirectory) {
        val files = dir.listFiles()
        if (files != null) {
          files.foreach { file =>
            if (file.isFile && file.getName.startsWith("shuffle_")) {
              fileCount += 1
              totalSizeBytes += file.length()
            } else if (file.isDirectory) {
              scanDir(file)
            }
          }
        }
      }
    }
    
    scanDir(directory)
    val totalSizeKB = totalSizeBytes / 1024
    (fileCount, totalSizeKB)
  }
  
  private def formatSizeKB(kb: Long): String = {
    if (kb < 1024) s"${kb}KB"
    else if (kb < 1024 * 1024) f"${kb / 1024.0}%.2fMB"
    else f"${kb / (1024.0 * 1024.0)}%.2fGB"
  }
  
  def stopMonitoring(): Unit = {
    isMonitoring = false
    executorService.shutdown()
    
    println(s"\n[ShuffleMonitor 最终报告]")
    println(s"峰值文件数: ${maxFileCount.get()}")
    println(s"峰值文件大小: ${formatSizeKB(maxTotalSize)}")
    
    // 根据文件数量判断Shuffle类型
    val estimatedType = if (maxFileCount.get() > 500) "Hash Shuffle（文件数>500）" else "Sort Shuffle"
    println(s"推测Shuffle类型: $estimatedType")
  }
  
  def getMaxFileCount: Int = maxFileCount.get()
  def getMaxTotalSizeKB: Long = maxTotalSize
}

class ShuffleMetricsListener extends SparkListener {
  private var shuffleWriteBytes: Long = 0L
  
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    if (metrics != null) {
      metrics.shuffleWriteMetrics.foreach { writeMetrics =>
        shuffleWriteBytes += writeMetrics.shuffleBytesWritten
      }
    }
  }
  
  def getShuffleWriteBytes: Long = shuffleWriteBytes
  def reset(): Unit = shuffleWriteBytes = 0L
}

object ShuffleExperiment {
  
  def runExperiment(df: DataFrame, sc: SparkContext, name: String): (Long, Long, Int, Long) = {
    val listener = new ShuffleMetricsListener
    sc.addSparkListener(listener)
    
    // 启动文件监控
    val monitor = new ShuffleFileMonitor()
    println(s"\n=== 开始运行 $name 实验 ===")
    println(s"启动Shuffle文件监控，每秒检查一次...")
    monitor.startMonitoring(intervalSeconds = 1, durationSeconds = 180)
    
    val startTime = System.currentTimeMillis()
    
    // 执行Shuffle操作

    // 小任务配置较少的分区
    // val shufflePartitions = 100

    // 大任务配置较多的分区
    val shufflePartitions = 500
    
    // DataGenerator 生成的列: 0:key, 1:value, 2:category, 3:payload
    val result = df.rdd
      .map { row => 
        (row.getString(0), row.getString(3)) 
      }
      .groupByKey(shufflePartitions)
      .count() // 触发 Action
    
    val endTime = System.currentTimeMillis()
    Thread.sleep(5000)

    val shuffleBytes = listener.getShuffleWriteBytes
    
    monitor.stopMonitoring()
    (endTime - startTime, shuffleBytes, monitor.getMaxFileCount, monitor.getMaxTotalSizeKB)
  }
  
  def formatBytes(bytes: Long): String = {
    val units = Array("B", "KB", "MB", "GB", "TB")
    if (bytes <= 0) return "0 B"
    val digitGroups = (Math.log10(bytes.toDouble) / Math.log10(1024)).toInt
    val unit = units(Math.min(digitGroups, units.length - 1))
    val value = bytes / Math.pow(1024, Math.min(digitGroups, units.length - 1))
    f"$value%.2f $unit"
  }
  
  def analyzeShuffleType(fileCount: Int, fileSizeKB: Long): String = {
    if (fileCount > 1000) s"Hash Shuffle (检测到大量小文件: $fileCount 个)"
    else if (fileCount <= 200) s"Sort Shuffle (文件高度合并: $fileCount 个)"
    else s"混合/不确定 ($fileCount 个文件)"
  }

  def main(args: Array[String]): Unit = {
    val mode = if (args.length > 0) args(0) else "sort"
    val datasetSize = if (args.length > 1) args(1) else "small"
    
    val conf = new SparkConf()
      .setAppName(s"Shuffle-File-Monitor-$mode")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.shuffle.compress", "false")
      .set("spark.shuffle.spill.compress", "false")
      // 增加缓冲区，让 Sort Shuffle 更有优势
      .set("spark.shuffle.file.buffer", "64k")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置本地目录以便监控
      .set("spark.local.dir", "/tmp/spark/work")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "/tmp/spark-events")

    // val env = SparkEnv.get
    // println(s"🔥🔥🔥 实际生效的 Shuffle Manager 类: ${env.shuffleManager.getClass.getCanonicalName}")

    val sc = new SparkContext(conf)
    try {
      val sqlContext = new SQLContext(sc)
      sc.setLogLevel("WARN")

      println(s"=== Shuffle File Monitor 实验 ===")
      println(s"3. Spark版本: ${sc.version}")
      println(s"默认shuffle分区数: ${sc.getConf.get("spark.sql.shuffle.partitions", "200")}")
      println(s"请求的Shuffle Manager: $mode")
      println(s"当前 Shuffle Manager: ${sc.getConf.get("spark.shuffle.manager", "sort")}")
      println(s"Spark 本地目录: ${sc.getConf.get("spark.local.dir", "/tmp/spark/work")}")
      println(s"监控将每秒检查一次 /tmp/spark/work 目录中的 shuffle_* 文件")
      
      // 清理旧的监控目录（可选）
      // val hadoopConf = sc.hadoopConfiguration
      // val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)


      // 只生成指定的一个数据集
      val df = datasetSize match {
        case "small-x" => DataGenerator.generateSmallX(sqlContext)
        case "small" => DataGenerator.generateSmall(sqlContext) // 你需要稍微改一下DataGenerator
        case "medium" => DataGenerator.generateMedium(sqlContext)
        case "large" => DataGenerator.generateLarge(sqlContext)
      }

      println(s"测试数据集: $datasetSize")
      // println(s"测试数据集V2: $size, 记录数: ${df.count()}")
      println(s"分区数: ${df.rdd.getNumPartitions}")

      // ==========================================
      // 新增：预热阶段 (解决 0 文件等待问题)
      // ==========================================
      println(">>> 正在进行数据预热 (Cache & Count)...")
      // 1. 将数据缓存到内存
      df.cache() 
      // 2. 强制触发一次计算，让 Executors 把数据真正在内存里生成好
      val warmupStart = System.currentTimeMillis()
      val count = df.count() 
      val warmupEnd = System.currentTimeMillis()
      println(s">>> 预热完成! 数据已驻留内存。耗时: ${warmupEnd - warmupStart}ms, 记录数: $count")
      println(">>> 现在开始正式 Shuffle 实验 (此时文件应该会立即产生)")

      val (time, bytes, fileCount, fileSizeKB) = runExperiment(df, sc, mode)
        
      println(s"\n" + "="*50)
      println(s"测试结果 [$mode]:")
      println(s"  执行耗时: ${time}ms")
      println(s"  Shuffle数据量: ${formatBytes(bytes)}")
      println(s"  峰值Shuffle文件数: $fileCount 个")
      println(s"  峰值Shuffle文件大小: ${fileSizeKB}KB (${"%.2f".format(fileSizeKB/1024.0)}MB)")
      println(s"  Shuffle类型分析: ${analyzeShuffleType(fileCount, fileSizeKB)}")
      
      // 生成文件分析报告
      if (fileCount > 0) {
        val avgFileSizeKB = if (fileCount > 0) fileSizeKB / fileCount else 0
        println(s"  平均每个文件大小: ${avgFileSizeKB}KB")
        
        // 根据文件特征给出建议
        if (fileCount > 500) {
          println(s"  ⚠️  警告: 检测到大量小文件($fileCount)个，建议使用Sort Shuffle")
        } else if (avgFileSizeKB < 100 && fileCount > 100) {
          println(s"  ℹ️  提示: 较多小文件，考虑调整spark.shuffle.file.buffer")
        }
      }
    } finally {
      sc.stop()
    }
  }
}