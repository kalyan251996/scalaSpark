import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object dataframemq1 {
  def main(args:Array[String]):Unit={
    val spark =SparkSession.builder().appName("Stoclevel").master("local[*]")getOrCreate()
    import spark.implicits._
    val inventory =List((1,5),(2,15),(3,25)).toDF("item_id","quantity")
    val df =inventory.withColumn("stock_level",when(col("quantity")<10,"low").when(col("quantity")<20
      &&col("quantity")>10,"Medium").otherwise("High"))
      df.show()
    df.printSchema()

  }

}
