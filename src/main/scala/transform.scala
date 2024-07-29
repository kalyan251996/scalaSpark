import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
object transform {
  def main(args:Array[String]):Unit={
    //Transformations Rdd's
    val spark =SparkSession.builder().appName("transformation").master("local[*]").getOrCreate()
    val schema=StructType(List(

      StructField("first_name",StringType),
      StructField("last_name",StringType),
      StructField("Country",StringType),
      StructField("State",StringType),
      StructField("age",IntegerType)

    ))
    val df =spark.read.format("csv").option("header",true).schema(schema)
                                    .option("path","D:/data/us_short.csv")
                                    .load()

    val df1=df.select(col("first_name"),col("last_name"),col("Country"),col("state"),col("age"),
      when(col("age")<18,"child").when(col("age")>18 && col("age")<30,"Adult").otherwise("Old")
          .alias("details"))
    df1.show()
  }

}
