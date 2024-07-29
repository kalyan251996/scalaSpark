import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}


object dataframeQ1 {
  def main(args: Array[String]): Unit = {
   val spark=SparkSession.builder().appName("age_Changes").master("local[*]").getOrCreate()
    import spark.implicits._
    val employees =List(
      (1, "John", 28),
      (2, "Jane", 35),
      (3, "Doe", 22)).toDF("id", "name", "age")
   val emp1 =employees.withColumn("Details",when(col("age")>=18,"Adult").otherwise("false"))
    emp1.show()
    emp1.printSchema()
 }
}
