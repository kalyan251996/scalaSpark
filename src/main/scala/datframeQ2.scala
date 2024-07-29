import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object datframeQ2 {
  def main(args:Array[String]):Unit={
    val spark=SparkSession.builder().appName("Amount_info").master("local[*]").getOrCreate()
    import spark.implicits._
    val grades = List(
      (1, 85),
      (2, 42),
      (3, 73)).toDF("student_id", "score")
    val df =grades.withColumn("Grade",when(col("score")>=50,"Pass" ).otherwise("Fail"))
    df.show()

  }

}
