import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object datframehq8 {
  def main(args:Array[String]):Unit={
    val spark =SparkSession.builder().appName("Email_provider").master("local[*]").getOrCreate()
    import spark.implicits._
    val customers=List((1,"user@gmail.com"),(2,"admin@yahoo.com"),(3,"info@hotmail.com")).toDF("customer_id","email")
    val df =customers.withColumn("Email_provider",when(col("email").contains("gmail"),"Gmail").
      when(col("email").contains("yahoo"),"yahoo").otherwise("Hotmail"))

}
  }
