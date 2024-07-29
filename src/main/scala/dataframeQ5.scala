import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object dataframeQ5 {
def main(args:Array[String]):Unit={
  val spark =SparkSession.builder().appName("Holiday").master("local[*]").getOrCreate()
  import spark.implicits._
  val events = List((1, "2024-07-27"),(2, "2024-12-25"),(3, "2025-01-01")).toDF("event_id", "date")
  val holiday =events.withColumn("Holiday",when(col("date").isin("2024-12-25","2025-01-01"),true )
                .otherwise(false))
    holiday.show()
  holiday.printSchema()
}
}
