import org.apache.spark.SparkContext
object setq3 {
  def main(args:Array[String]):Unit={
    //SensorData
    val sc= new SparkContext("local[*]","SensorData")
    val rdd1 = sc.textFile("D:/data/orderdata.txt")
    val rdd2= rdd1.map(x=>(x.split(",")(1),x.split(",")(3).toFloat))
    val rdd3=rdd2.reduceByKey( (x,y)=>x+y)
  rdd3.collect.foreach(println)
  }

}
