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
					
					
/*
 * 
------------------------------
Test each Mode separately 
-----------------------------

val df = spark.read.format("json").load("file:///C:/data/devices.json")

val finaldf = df.filter("humidity>40")

==========
Run it  ---- It would be successful
==========
finaldf.write.format("csv").mode("error").save("file:///C:/data/dhumid")


==========
Run it  --- It will through Error
==========

finaldf.write.format("csv").mode("error").save("file:///C:/data/dhumid")

==========
Run it  --- it will append the data
==========

finaldf.write.format("csv").mode("append").save("file:///C:/data/dhumid")

==========
Run it  --- it will overwrite the data
==========

finaldf.write.format("csv").mode("overwrite").save("file:///C:/data/dhumid")

==========
Run it  --- it will ignore the data
==========

finaldf.write.format("csv").mode("ignore").save("file:///C:/data/dhumid")
 * 
 */
					
/*     
 *     In summary:----   
 *          finaldf.write.format("csv").mode("error").save("file:///C:/data/dhumid")
 *          finaldf.write.format("csv").mode("append").save("file:///C:/data/dhumid")
 *          finaldf.write.format("csv").mode("overwrite").save("file:///C:/data/dhumid")          
 */					
					
				
           println("===== dataframe of file(devices.json) =====")					
					val df = spark.read.format("json").load("file:///C:/data/devices.json")
					df.show()
					
					println("===== dataframe of file(devices.json) where humidity>40 =====")
          val finaldf = df.filter("humidity>40")
          finaldf.show()
          
          
          
          println("=== dhumid write on c:/data/ in ignore mode ======")
          finaldf.write.format("csv").mode("ignore").save("file:///C:/data/dhumid")

					println("====mode ignore======")
					
	  }
	}
