import org.apache.spark.SparkContext
object wcount {
    def main(args: Array[String]): Unit = {
      val sc = new SparkContext("local[*]", "kalyan")
      val rdd1 = sc.textFile("D:/data/ipadd.txt")
      val rdd2 = rdd1.flatMap(x => x.split("/"))
      val rdd3 = rdd2.map(x => (x, 1))
      val rdd4 = rdd3.reduceByKey((x, y) => x + y)
     rdd4.collect.foreach(println)

    }


}
