import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.functions.{col,when,avg,sum,max,count,min}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SetaDfQ1 {
  def main(args:Array[String]):Unit={
    val spark =SparkSession.builder().appName("Student_Analysis").master("local[*]").getOrCreate()
    import spark.implicits._
//    val students=spark.read.format("csv").option("header",true).option("path","D:/Data/students.txt").load()
    val students =List((1,"Alice",92,"Math"),
      (2,"Bob",85,"Math"),
      (3,"Carol",77,"Science"),
      (4,"Dave",65,"Science"),
      (5,"Eve",50,"Math"),
      (6,"Frank",82,"Science")).toDF("student_id","name","score","subject")
    val gradedf =students.withColumn("grades",when(col("score")>90,"A").when(col("score")<90 && col("score")>=80,"B")
                                              .when(col("score")<80 && col("score")>=70,"C")
                                              .when(col("score")<70 && col("score")>=60,"D")
                                              .when(col("score")<60,"F"))
    gradedf.show()
    val groupdf =students.groupBy(col("subject"))
    val avgdf =groupdf.agg(avg(col("score")))
    val maxdf =groupdf.agg(max(col("score")))
    val mindf =groupdf.agg(min(col("score")))
    val countst =gradedf.groupBy(col("subject"),col("grades")).agg(count(col("grades")).alias("totalstudents"))
    avgdf.show()
    maxdf.show()
    mindf.show()
    countst.show()



  }

}
