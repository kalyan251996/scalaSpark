import org.apache.spark.sql.functions.{col,when,avg,sum,max,count}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}


object todfexaam {
  def main(args:Array[String]):Unit={
    //Full Dataframe Concepts
    val spark=SparkSession.builder().appName("toDf_example").master("local[*]").getOrCreate()
    val df5=spark.read.format("csv").option("header",true).option("path","D:/data/transact.txt").load()
    val df6 =df5.withColumn("ammount_info",when(col("amount")>1000,"high").
      when(col("amount")<1000 && col("amount")>500,"high").otherwise("low"))
    df6.show()
    import spark.implicits._
    val dataframe=List(("mohan",28),("Mahi",78)).toDF("Name","Age")
        dataframe.show()
    val employees = List(
                    (1, 25, 30000),
                    (2, 45, 50000),
                    (3, 35, 39000)).toDF("employee_id", "age", "salary")
val df1= employees.withColumn("Category",when(col("age")<30 && col("salary")<350000,"Young& Low Salary").
    when(col("age")> 30 && col("age")<40 && col("salary")>350000 || col("salary")<40000,"medium & Medium Salary")
    .when(col("age")>40  && col("salary")>40000 || col("salary")<40000,"OLD AND HIGH Salary"))
df1.show(false)
val scoreData = Seq(
  ("Alice", "Math", 80),
  ("Bob", "Math", 90),
  ("Alice", "Science", 70),
  ("Bob", "Science", 85),
  ("Alice", "English", 75),
  ("Bob", "English", 95)).toDF("Student", "Subject","Score")
    val df2 =scoreData.groupBy(col("Subject")).agg(avg(col("Score")).alias("Avg_score"))
    df2.show()
    val df3 =scoreData.groupBy(col("Student")).agg(max(col("Score")).alias("Max_Score"))
    df3.show()

    val ratingsData = Seq(
      ("User1", "Movie1", 4.5),
      ("User2", "Movie1", 3.5),
      ("User3", "Movie2", 2.5),
      ("User4", "Movie2", 3.0),
      ("User1", "Movie3", 5.0),
      ("User2", "Movie3", 4.0)
    ).toDF("User", "Movie","Rating")
    val df4 = ratingsData.groupBy(col("Movie"))
      df4.agg(avg(col("Rating")).alias("Average_rating")).show()
      df4.agg(count(col("Rating")).alias("total_ratings")).show()
  }
}

