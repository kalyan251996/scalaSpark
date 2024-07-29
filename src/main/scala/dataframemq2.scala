import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object dataframemq2 {
  def main(args:Array[String]):Unit={
    val spark =SparkSession.builder().appName("Email_provider").master("local[*]").getOrCreate()
    import spark.implicits._
    val customers=List((1,"john@gmail.com"),(2,"jane@yahoo.com"),(3,"doe@hotmail.com")).toDF("customer_id","email")
    val df =customers.withColumn("Email_provider",when(col("email").contains("gmail"),"Gmail").
      when(col("email").contains("yahoo"),"yahoo").otherwise("Hotmail"))

    df.show()
  }

}
