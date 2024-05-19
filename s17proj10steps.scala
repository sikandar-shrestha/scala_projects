package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io._

object s17proj10steps {
  
  /*--------------------
 10 steps--Project
---------------------

1) Create an eclipse project
2) Add avro jar and read projectsample.avro
3) Read URL data and convert that to Dataframe

	https://randomuser.me/api/0.8/?results=10

4) Flatten the URL and do not select (COLUMN CAPS)

5) Remove numericals from username column

6) Do left broadcast join (avrodf and numerical removed url df)

7) From dataframe of 6th Step -- Create two dataframes

	val availableCustomer    ==== nationality is not null
	val notavailabelcustomer ==== nationality is  null 
	
8) Null handled dataframe

9) 9a. Not  available customers with current data dataframe.
	9b. Available customers with current data dataframe.
	
10) Write it as Parquet.

*/

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

val spark = SparkSession.builder()

      .getOrCreate()

    import spark.implicits._
    

    println
    println
    println
    println
    println
    println("======================= Step 2 ========Raw data=============================================")
    println
    println
    println
    println
    println

    val data = spark.read.format("com.databricks.spark.avro")
      .load("file:///C://data//projectsample.avro")

    data.show()

    println
    println
    println
    println
    println
    println("======================== Step 3 ========Url data=============================================")
    println
    println
    println
    println
    println

    val html = Source.fromURL("https://randomuser.me/api/0.8/?results=100")
    val s = html.mkString
    println(s)

    val urldf = spark.read.json(sc.parallelize(List(s)))
    urldf.show()
    urldf.printSchema()

    println
    println
    println
    println
    println
    println("========================step 4 flatten dataframe=============================================")
    println
    println
    println
    println
    println

    val flatdf = urldf.withColumn("results", explode(col("results"))).select("nationality", "seed", "version",
      "results.user.username", "results.user.cell", "results.user.dob", "results.user.email",
      "results.user.gender", "results.user.location.city", "results.user.location.state",
      "results.user.location.street", "results.user.location.zip", "results.user.md5",
      "results.user.name.first", "results.user.name.last", "results.user.name.title",
      "results.user.password", "results.user.phone", "results.user.picture.large", "results.user.picture.medium", "results.user.picture.thumbnail", "results.user.registered", "results.user.salt", "results.user.sha1", "results.user.sha256")
    flatdf.show()

    println
    println
    println
    println
    println
    println("========================step 5 removed numericals Dataframe=============================================")
    println
    println
    println
    println
    println

    val rm = flatdf.withColumn("username", regexp_replace(col("username"), "([0-9])", ""))
    rm.show()

    println
    println
    println
    println
    println
    println("====================== Step 6 =========Joined Dataframe=============================================")
    println
    println
    println
    println
    println
    val joindf = data.join(broadcast(rm), Seq("username"), "left")

    joindf.show()

    println
    println
    println
    println
    println("=================== Step 7 a ============Not available customers=============================================")
    println
    println
    println
    println
    println
    println

    val dfnull = joindf.filter(col("nationality").isNull)

    val dfnotnull = joindf.filter(col("nationality").isNotNull)
    dfnull.show()
    println
    println
    println
    println
    println("==================  Step 7 b =============available customers=============================================")
    dfnotnull.show()
    println
    println
    println
    println
    println
    println
    println("=============== Step 8 ================Null handled dataframe=============================================")
    println
    println
    println
    println
    println
    println

    val replacenull = dfnull.na.fill("Not Available").na.fill(0)
    replacenull.show()

    println
    println
    println
    println
    println("=============== Step 9 a ================not available customers with current date dataframe=============================================")
    println
    println
    println
    println
    println
    println

    val replacenull_with_current_date = replacenull.withColumn("current_date", current_date)

    replacenull_with_current_date.show()

    println
    println
    println
    println
    println
    println("=============== Step 9 b ================available customers with current date dataframe====================================")
    println
    println
    println
    println
    println
    println

    val notnull_with_current_date = dfnotnull.withColumn("current_date", current_date)

    notnull_with_current_date.show()
    
   println("=============== Step 10 : Write as Parquet format====================================================")

    

    notnull_with_current_date.write.format("parquet").mode("append").partitionBy("current_date")
      .save("file:///C:/data/project/availablecustomer")

    replacenull_with_current_date.write.format("parquet").mode("append").partitionBy("current_date")
      .save("file:///C:/data/project/notavailablecustomer")

  }
}