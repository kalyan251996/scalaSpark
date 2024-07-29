import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object Dataframehq6 {
  def main(args:Array[String]):Unit={
    val spark =SparkSession.builder().appName("Humid_nature").master("local[*]").getOrCreate()
    import spark.implicits._
    val weather = Seq(
      (1, 25, 60),
      (2, 35, 40),
      (3, 15, 80)).toDF("day_id", "temperature", "humidity")
    val df =weather.withColumn("is_hot",when(col("temperature")>30,true).otherwise(false))
                    .withColumn("is_humid",when(col("temperature")<30, true).otherwise(false))
    df.show()
  }

}
