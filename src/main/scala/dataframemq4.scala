import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object dataframemq4 {
  def main(args:Array[String]):Unit={
    val spark= SparkSession.builder.appName("Discount").master("local[*]").getOrCreate()
    import spark.implicits._
    val sales = List(
      (1, 100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")
    val df =sales.withColumn("DiscountDetails", when(col("amount")<200,0).when(col("amount")<1000 &&col("amount")>200,10)
      .otherwise(20))
    df.show()
    df.printSchema()
  }
}






