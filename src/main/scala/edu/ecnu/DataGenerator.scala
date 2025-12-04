package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.SparkContext
import scala.util.Random

object DataGenerator {

  /**
   * 构建 Zipf 累积分布函数 (CDF)
   * 用于生成符合 Zipf 分布的随机索引
   */
  def buildZipfCDF(numKeys: Int, skew: Double): Array[Double] = {
    val weights = (1 to numKeys).map(k => 1.0 / math.pow(k, skew)).toArray
    val sum = weights.sum
    val normalized = weights
        .scanLeft(0.0)(_ + _)
        .map(_ / sum)
    normalized.tail
  }

  /**
   * 基于 CDF 进行采样
   */
  def zipfSample(cdf: Array[Double]): Int = {
    val r = Random.nextDouble()
    cdf.indexWhere(r <= _) match {
      case -1 => cdf.length - 1
      case idx => idx
    }
  }
  
  /**
   * 生成用户行为数据
   * @param numRecords 记录条数
   * @param numPartitions RDD分区数
   * @param skew 倾斜度 (0.0 = 均匀/UUID, >0.0 = Zipf倾斜)
   */
  def generateUserBehavior(sqlContext: SQLContext, numRecords: Long, numPartitions: Int, skew: Double): DataFrame = {
    import sqlContext.implicits._
    
    // 定义 Key 的空间大小 (用于 Zipf 采样)
    val numKeys = 100000 
    
    // 如果需要倾斜，在 Driver 端预计算 CDF 并广播，避免 Task 重复计算
    val zipfCDF = if (skew > 0) buildZipfCDF(numKeys, skew) else null
    val bcZipfCDF = if (skew > 0) sqlContext.sparkContext.broadcast(zipfCDF) else null

    val rdd = sqlContext.sparkContext.parallelize(1L to numRecords, numPartitions).map { id =>
      val rnd = new Random()
      
      // 生成 Payload (1KB)
      val payloadTemplates = (1 to 10).map { _ => 
        val bytes = new Array[Byte](1024) 
        rnd.nextBytes(bytes)
        new String(bytes, "ISO-8859-1") 
      }.toArray
      val bigData = payloadTemplates(rnd.nextInt(payloadTemplates.length))

      // === 核心修改逻辑 ===
      val key = if (skew > 0) {
         // 倾斜模式: 使用 Zipf 分布采样 Key
         val rank = zipfSample(bcZipfCDF.value)
         f"key_$rank%08d" 
      } else {
         // 均匀模式: 使用 UUID
         java.util.UUID.randomUUID().toString
      }

      val value = rnd.nextDouble() * 1000
      
      (key, value, "category_placeholder", bigData)
    }
    
    sqlContext.createDataFrame(rdd).toDF("key", "value", "category", "payload")
  }

  // === 各种规模的工厂方法 (透传 skew 参数) ===

  def generateSmallX(sqlContext: SQLContext, skew: Double): DataFrame = {
    generateUserBehavior(sqlContext, 10000L, 5, skew)
  }

  def generateSmall(sqlContext: SQLContext, skew: Double): DataFrame = {
    generateUserBehavior(sqlContext, 100000L, 10, skew) 
  }

  def generateMedium(sqlContext: SQLContext, skew: Double): DataFrame = {
    generateUserBehavior(sqlContext, 1000000L, 50, skew)
  }

  def generateLarge(sqlContext: SQLContext, skew: Double): DataFrame = {
    generateUserBehavior(sqlContext, 5000000L, 200, skew)
  }
  
  /**
   * 统一入口
   */
  def generate(sqlContext: SQLContext, size: String, skew: Double): DataFrame = {
    size.toLowerCase match {
      case "small-x" => generateSmallX(sqlContext, skew)
      case "small"  => generateSmall(sqlContext, skew)
      case "medium" => generateMedium(sqlContext, skew)
      case "large"  => generateLarge(sqlContext, skew)
      case _ => 
        println(s"警告: 未知的数据集大小 '$size'，默认使用 small")
        generateSmall(sqlContext, skew)
    }
  }

  /**
   * 加载 TPC-H 数据并根据场景构建 DataFrame
   * * @param inputPath TPC-H lineitem.tbl 的路径 (例如 file:///tmp/tpch/lineitem.tbl)
   * @param scenario  实验场景: "uniform" (基准), "longtail" (长尾), "skew" (倾斜)
   * @return DataFrame schema: [key, placeholder, placeholder, payload] 
   * (保持4列是为了兼容 ShuffleExperiment 中 row.getString(3) 的取值逻辑)
   */
  def loadTpch(sqlContext: SQLContext, inputPath: String, scenario: String): DataFrame = {
    import sqlContext.implicits._
    val sc = sqlContext.sparkContext

    // 1. 读取原始文本
    // 关键点：本地读取通常只有1个分区，必须 repartition 才能模拟并发 Shuffle
    val rawRdd = sc.textFile(inputPath)
      .repartition(100) // 建议根据机器核数调整，比如 2 * cores 或固定 100

    val processedRdd = rawRdd.map { line =>
      val parts = line.split("\\|")
      // lineitem.tbl 常用字段索引:
      // 0: L_ORDERKEY, 1: L_PARTKEY, 2: L_SUPPKEY
      
      val key = scenario.toLowerCase match {
        case "uniform" =>
          // === 场景1: 基准 (Uniform) ===
          // 使用 L_ORDERKEY，分布非常均匀
          parts(0)

        case "longtail" =>
          // === 场景2: 长尾/高基数 (High Cardinality) ===
          // 组合 L_PARTKEY + L_SUPPKEY
          // Key 的数量极大，测试 Hash Shuffle 的 Map 端内存压力
          parts(1) + "_" + parts(2)

        case "skew" =>
          // === 场景3: 数据倾斜 (Skew) ===
          // 人为制造倾斜：让 20% 的数据集中到一个 Key
          val orderKey = try { parts(0).toLong } catch { case _: Exception => 0L }
          if (orderKey % 5 == 0) "HOT_SPOT_KEY" else parts(0)

        case _ => parts(0)
      }

      // 为了兼容 ShuffleExperiment 中的 row.getString(3)，我们需要构造 4 列
      // (Key, Pad1, Pad2, Payload/Value)
      (key, "pad", "pad", line) 
    }

    sqlContext.createDataFrame(processedRdd)
      .toDF("key", "pad1", "pad2", "payload")
  }
}
