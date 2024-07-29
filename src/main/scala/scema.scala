import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object scema {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .appName("spark-program")
      .master("local[*]")
      .getOrCreate()





    //    val schema=" id Int,Name String,Age Int,Salary Int,city String,details String,mean Int"

    val schema=StructType(List(

      StructField("id",IntegerType),
      StructField("Name",StringType),
      StructField("Age",IntegerType),
      StructField("Salary",IntegerType),
      StructField("city",StringType),
      StructField("details",StringType),
      StructField("mean",IntegerType)

    ))

    val df=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema)
      .option("path","D:/data/info.csv")
      .load()

    df.show(false)
    df.printSchema()


}
}
