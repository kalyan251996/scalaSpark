import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object datframehq1 {
  def main(args:Array[String]):Unit={
    val spark= SparkSession.builder().appName("Category_age").master("local[*]").getOrCreate()
    import spark.implicits._
    val employees = List(
      (1, 25, 30000),
      (2, 45, 50000),
      (3, 35, 39000)).toDF("employee_id", "age", "salary")
    val df1= employees.withColumn("Category",when(col("age")<30 && col("salary")<350000,"Young& Low Salary").
      when(col("age")> 30 && col("age")<40 && col("salary")>350000 || col("salary")<40000,"medium & Medium Salary")
      .when(col("age")>40  && col("salary")>40000 || col("salary")<40000,"OLD AND HIGH Salary"))
    df1.show(false)
  }

}
