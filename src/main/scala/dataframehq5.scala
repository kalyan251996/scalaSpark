import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object dataframehq5 {
def main(args:Array[String]):Unit={
  val spark =SparkSession.builder().appName("Qunt_Disc").master("local[*]").getOrCreate()
  import spark.implicits._
  val orders = List(
    (1, 5, 100),
    (2, 10, 150),
    (3, 20, 300)).toDF("order_id", "quantity", "total_price")
  val df =orders.withColumn("order_type",when(col("quantity")<10 && col("total_price")<200,"Small & Cheap" )
  .when(col("quantity")>=10 && col("total_price")<200 ,"Bulk & Discount").otherwise("Premium Order"))
  df.show()
}
}
