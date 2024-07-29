import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object dataframehq2 {
  def main(args:Array[String]):Unit={
    val spark =SparkSession.builder().appName("review Def").master("local[*]").getOrCreate()
    import spark.implicits._
    val reviews =List((1,1),(2,4),(3,5)).toDF("review_id","rating")
    val df =reviews.withColumn("ReviewCat",when(col("rating")<3,"Bad").when(col("rating")<=4 && col("rating")>=3,"Good")
    .otherwise("Excellent"))
    df.show()
  }

}
