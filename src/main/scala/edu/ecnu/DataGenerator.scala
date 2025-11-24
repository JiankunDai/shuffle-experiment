package edu.ecnu

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.SparkContext
import scala.util.Random

object DataGenerator {
  
  def generateUserBehavior(sqlContext: SQLContext, numRecords: Long): DataFrame = {
    import sqlContext.implicits._
    
    val categories = Array("electronics", "clothing", "books", "home", "sports")
    val regions = Array("north", "south", "east", "west", "central")
    
    // 创建RDD然后转换为DataFrame
    val rdd = sqlContext.sparkContext.parallelize(1L to numRecords).map { id =>
      val userId = Random.nextInt(100000)
      val itemId = Random.nextInt(5000)
      val category = categories(Random.nextInt(categories.length))
      val region = regions(Random.nextInt(regions.length))
      val timestamp = System.currentTimeMillis() - Random.nextInt(1000000)
      val value = (Random.nextDouble() * 1000)
      
      (userId, itemId, category, timestamp, value, region)
    }
    
    sqlContext.createDataFrame(rdd).toDF("userId", "itemId", "category", "timestamp", "value", "region")
  }
  
  def generateDatasets(sqlContext: SQLContext): Map[String, DataFrame] = {
    Map(
      "small" -> generateUserBehavior(sqlContext, 100000L),    // 10万记录
      "medium" -> generateUserBehavior(sqlContext, 1000000L),  // 100万记录
      "large" -> generateUserBehavior(sqlContext, 50000000L)    // 5000万记录
    )
  }
}