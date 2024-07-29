import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object dataframeQ4 {
def main(args:Array[String]):Unit={
  val spark = SparkSession.builder().appName("Product Category ").master("local[*]").getOrCreate()
  import spark.implicits._
  val products= List((1, 30.5),(2, 150.75),(3, 75.25)).toDF("product_id", "price")
  val df =products.withColumn("Product_cat",when(col("price")<50,"Cheap")
          .when(col("price")>50 && col("price")<100,"Moderate").otherwise("Expensive"))
  df.show()

}
}
