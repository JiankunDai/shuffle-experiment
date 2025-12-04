package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkContext, SparkConf, SparkEnv}
import org.apache.spark.scheduler._
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class ShuffleFileMonitor(sparkLocalDir: String = "/tmp/spark/work") {
  private val executorService = Executors.newSingleThreadExecutor()
  private var isMonitoring = false
  private val maxFileCount = new AtomicInteger(0)
  private var maxTotalSize: Long = 0L
  
  def startMonitoring(intervalSeconds: Int = 1, durationSeconds: Int = 600): Unit = {
    isMonitoring = true
    val startTime = System.currentTimeMillis()
    
    executorService.submit(new Runnable {
      override def run(): Unit = {
        while (isMonitoring && System.currentTimeMillis() - startTime < durationSeconds * 1000) {
          try {
            val shuffleDir = new File(sparkLocalDir)
            val (fileCount, totalSizeKB) = if (shuffleDir.exists()) countShuffleFiles(shuffleDir) else (0, 0L)
            
            if (fileCount > maxFileCount.get()) maxFileCount.set(fileCount)
            if (totalSizeKB > maxTotalSize) maxTotalSize = totalSizeKB
            
            Thread.sleep(intervalSeconds * 1000)
          } catch { case e: Exception => }
        }
      }
    })
  }
  
  private def countShuffleFiles(directory: File): (Int, Long) = {
    var fileCount = 0
    var totalSizeBytes: Long = 0L
    def scanDir(dir: File): Unit = {
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
    scanDir(directory)
    (fileCount, totalSizeBytes / 1024)
  }
  
  private def formatSizeKB(kb: Long): String = {
    if (kb < 1024) s"${kb}KB" else if (kb < 1024*1024) f"${kb/1024.0}%.2fMB" else f"${kb/(1024.0*1024.0)}%.2fGB"
  }
  
  def stopMonitoring(): Unit = {
    isMonitoring = false
    executorService.shutdownNow()
  }
  
  def getMaxFileCount: Int = maxFileCount.get()
  def getMaxTotalSizeKB: Long = maxTotalSize
}

class ShuffleMetricsListener extends SparkListener {
  private var shuffleWriteBytes: Long = 0L
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    if (metrics != null) metrics.shuffleWriteMetrics.foreach(m => shuffleWriteBytes += m.shuffleBytesWritten)
  }
  def getShuffleWriteBytes: Long = shuffleWriteBytes
}

object ShuffleExperiment {
  
  def runExperiment(df: DataFrame, sc: SparkContext, name: String): (Long, Long, Int, Long) = {
    val listener = new ShuffleMetricsListener
    sc.addSparkListener(listener)
    val monitor = new ShuffleFileMonitor()
    monitor.startMonitoring()
    
    println(s"\n=== 开始运行 $name 实验 ===")
    val startTime = System.currentTimeMillis()
    
    // 使用 groupByKey 产生 Shuffle
    val result = df.rdd
      .map { row => (row.getString(0), row.getString(3)) }
      .groupByKey(200) // 强制 200 个 Reduce 分区
      .count()
    
    val endTime = System.currentTimeMillis()
    Thread.sleep(5000) 
    
    monitor.stopMonitoring()
    (endTime - startTime, listener.getShuffleWriteBytes, monitor.getMaxFileCount, monitor.getMaxTotalSizeKB)
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
    val mode = if (args.length > 0) args(0) else "sort"
    val datasetSize = if (args.length > 1) args(1) else "small"
    // 新增：接收第3个参数作为 Skew 倾斜度
    val skew = if (args.length > 2) args(2).toDouble else 0.0
    
    val conf = new SparkConf()
      .setAppName(s"Shuffle-Exp-$mode-$datasetSize-skew$skew")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.local.dir", "/tmp/spark/work")

    val sc = new SparkContext(conf)
    try {
      val sqlContext = new SQLContext(sc)
      sc.setLogLevel("WARN")

      println(s"=== Shuffle 实验配置 ===")
      println(s"Manager: $mode")
      println(s"Dataset: $datasetSize")
      println(s"Skew:    $skew (0=Uniform, >0=Zipf)")
      
      // 生成数据 (传入 skew 参数)
      val df = DataGenerator.generate(sqlContext, datasetSize, skew)
      
      println(">>> 正在预热 (Cache & Count)...")
      df.cache()
      println(s">>> 预热完成，记录数: ${df.count()}")
      
      val (time, bytes, fileCount, fileSizeKB) = runExperiment(df, sc, s"$mode-skew$skew")
      
      println(s"\nRESULT_CSV: $mode,$datasetSize,$skew,$time,${formatBytes(bytes)},$fileCount,${fileSizeKB}KB")
      
    } finally {
      sc.stop()
    }
  }
}
