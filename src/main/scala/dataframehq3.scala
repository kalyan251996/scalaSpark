import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object dataframehq3 {
  def main(args:Array[String]):Unit={
    val spark =SparkSession.builder().appName("Doc_category").master("local[*]").getOrCreate()
    import spark.implicits._
    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")).toDF("doc_id", "content")

    val df =documents.withColumn("Doc_details",when(col("content").contains("fox"),"Animal Related")
      .when(col("content").contains("Lorem"),"Placeholder Text").otherwise("Tech Related"))
    df.show(false)

  }

}
