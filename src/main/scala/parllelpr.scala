import org.apache.spark.SparkContext
object parllelpr {
  def main(args:Array[String]):Unit={

    val sc= new SparkContext("local[*]","parllize Method")
    val data = Array(1,2,3,4,5,6,7,8)
    val rdd1=sc.parallelize(data)
    rdd1.collect.foreach(println)
  }

}
