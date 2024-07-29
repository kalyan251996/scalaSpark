import org.apache.spark.SparkContext
object rddjoin {
  def main(args:Array[String]):Unit={

    val sc= new SparkContext("local[*]","rdd_join")
    val data1 =sc.parallelize(Array(("apple"),("bananna"),("Orange")))
    val searchterm="an"
    val rdd1=    data1.filter(x => x.contains(searchterm))
      rdd1.collect.foreach(println)


  }
}
