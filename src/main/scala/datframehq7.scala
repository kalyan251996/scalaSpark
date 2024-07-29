import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object datframehq7 {
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().appName("Student_Grade").master("local[*]").getOrCreate()
    import spark.implicits._
    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)).toDF("student_id", "math_score", "english_score")
    val df =scores.withColumn("Math_Grade",when(col("math_score")>80,"A")
                                           .when(col("math_score")<80 &&col("math_score")>60 ,"B").otherwise("C"))
                  .withColumn("English_Grade",when(col("english_score")>80,"A")
                                          .when(col("english_score")<80 &&col("english_score")>60 ,"B").otherwise("C"))
    df.show()

  }

}
