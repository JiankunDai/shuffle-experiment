package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.SparkContext
import scala.util.Random

object DataGenerator {
  
  /**
   * 生成用户行为数据
   * @param numRecords 记录条数
   * @param numPartitions 显式指定RDD的分区数，这对Hash Shuffle的文件数量至关重要
   */
  def generateUserBehavior(sqlContext: SQLContext, numRecords: Long, numPartitions: Int): DataFrame = {
    import sqlContext.implicits._
    
    // val categories = Array("electronics", "clothing", "books", "home", "sports")
    // val regions = Array("north", "south", "east", "west", "central")
    
    // 关键修改：parallelize 的第二个参数指定了分区数
    // 如果不指定，默认通常只有 2 (取决于CPU核数)，会导致 Hash Shuffle 只能产生很少的文件
    val rdd = sqlContext.sparkContext.parallelize(1L to numRecords, numPartitions).map { id =>
      val rnd = new Random()
      // 预先生成一个随机的 byte 数组作为 payload，避免每次循环都生成带来的 CPU 压力
      // 但为了防止压缩，我们准备几个不同的模版轮询使用
      val payloadTemplates = (1 to 10).map { _ => 
        val bytes = new Array[Byte](1024) // 1KB
        rnd.nextBytes(bytes)
        new String(bytes, "ISO-8859-1") //以此编码转string保持长度
      }.toArray

      val key = java.util.UUID.randomUUID().toString
      val value = rnd.nextDouble() * 1000
      // 随机选一个模版
      val bigData = payloadTemplates(rnd.nextInt(payloadTemplates.length))
      
      (key, value, "category_placeholder", bigData)
    }
    
    sqlContext.createDataFrame(rdd).toDF("key", "value", "category", "payload")
  }


  def generateSmallX(sqlContext: SQLContext): DataFrame = {
    // 10万条 * 1KB ≈ 100MB 数据
    // 10 分区 -> Hash产生 2000 个文件 (每个约 50KB)
    generateUserBehavior(sqlContext, 10000L, 5)
  }

  // 定义不同规模数据集的配置
  def generateSmall(sqlContext: SQLContext): DataFrame = {
    // 10万条 * 1KB ≈ 100MB 数据
    // 10 分区 -> Hash产生 2000 个文件 (每个约 50KB)
    generateUserBehavior(sqlContext, 100000L, 10) 
  }

  def generateMedium(sqlContext: SQLContext): DataFrame = {
    // 100万条 * 1KB ≈ 1GB 数据
    // 50 分区 -> Hash产生 10000 个文件 (每个约 100KB)
    // 这时候 Hash Shuffle 的写文件速度会开始明显变慢
    generateUserBehavior(sqlContext, 1000000L, 50)
  }

  def generateLarge(sqlContext: SQLContext): DataFrame = {
    // 500万条 * 1KB ≈ 5GB 数据
    // 100 分区 -> Hash产生 20000 个文件
    // 这可能会把你的磁盘打满或非常慢，非常适合做压力测试
    generateUserBehavior(sqlContext, 5000000L, 200)
  }
  
  /**
   * 🏭 工厂方法：根据传入的 size 字符串动态生成数据
   * 替代原来的 generateDatasets Map 方式，避免一次性生成所有数据导致内存溢出
   */
  def generate(sqlContext: SQLContext, size: String): DataFrame = {
    size.toLowerCase match {
      case "small-x" => generateSmallX(sqlContext)
      case "small"  => generateSmall(sqlContext)
      case "medium" => generateMedium(sqlContext)
      case "large"  => generateLarge(sqlContext)
      case _ => 
        println(s"警告: 未知的数据集大小 '$size'，默认使用 small")
        generateSmall(sqlContext)
    }
  }
}