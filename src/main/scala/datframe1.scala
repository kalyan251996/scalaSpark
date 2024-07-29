import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object datframe1
{
  def main(args:Array[String]):Unit={
//    val spark =SparkSession.builder()
//      .appName("DataFrame Programe")
//      .master("local[*]")
//      .getOrCreate()
//
  val sparkconfig = new SparkConf()
  sparkconfig.set("Spark.app.name","basic datarame Programe")
    sparkconfig.set("spark.master","local[*]")
    val spark =SparkSession.builder()
                .config(sparkconfig)
                .getOrCreate()
    val df =spark.read.format("csv").option("path","D:/data/txns_head").option("header",true).load()
    df.show(10)
    df.printSchema()
   }

}
