import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object dataframeQ3 {
  def main(args:Array[String]):Unit={
    //Print The high and low category based on  transaction amount
val spark =SparkSession.builder().appName("TransactionGroup").master("local[*]").getOrCreate()
    import spark.implicits._
    val transaction= List((1,1000),(2,200),(3,5000)).toDF("transaction_id","amount")
    val df= transaction.withColumn("Category",when(col("amount")> 1000,"High").
                when(col("amount")>50 && col("amount")<1000,"Medium").otherwise("Low"))
    df.show()
    df.printSchema()

  }

}
