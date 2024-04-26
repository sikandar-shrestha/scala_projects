package pack

import org.apache.spark.SparkContext  // rdd
import org.apache.spark.sql.SparkSession  // dataframe
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import pack._

object sess9mode {


	def main(args:Array[String]):Unit={
	  
	  
	      


			    val conf = new SparkConf().setMaster("local[*]").setAppName("first")

					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder.getOrCreate()
					import spark.implicits._
					
					
					
				
					
					val df = spark.read.format("json").load("file:///C:/data/devices.json")
					df.show()
					
          val finaldf = df.filter("humidity>40")
          finaldf.show()
          
          /*         
           *          finaldf.write.format("csv").mode("error").save("file:///C:/data/dhumid")
           *          finaldf.write.format("csv").mode("append").save("file:///C:/data/dhumid")
           *          finaldf.write.format("csv").mode("overwrite").save("file:///C:/data/dhumid")          
           */

          finaldf.write.format("csv").mode("ignore").save("file:///C:/data/dhumid")

					println("====mode ignore======")
					
	  }
	}
