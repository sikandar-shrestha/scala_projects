/*
-------------------------------------------------------------------
--------------------------------------------------------------------
E-Mail:
Boss:--you have a data in C drive C:/data/usdata.csv	
       Can you check and get back.
Sai:-- Yes, boss i have checked.
Boss:--1)First read this Data  Done
       2)Filter the Rows which has length>200 Done
       3)Flatten the data with ,Done
       4)Remove hyphon (-) from all the flatten Rows Done
       5)Concat ,zeyo for each string ,Done
       6)Write the results to a file
---------------------------------------------------------------------
---------------------------------------------------------------------
*/
package pack

import org.apache.spark._

object ses6txtph2{
	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val data = sc.textFile("file:///C:/data/usdata.csv")

					println("=====Raw data==== ")
					data.take(10).foreach(println)
					println
					
					val fildata = data.filter( x => x.length() > 200)
					println("=====fildata data==== ")
					fildata.foreach(println)
					println
			
					val flatdata = fildata.flatMap( x => x.split(","))
					println("=====flatdata data==== ")
					flatdata.foreach(println)
					println
					
					val repdata = flatdata.map( x => x.replace("-",""))
					println("=====repdata data==== ")
					repdata.foreach(println)
					println
					
					val condata = repdata.map( x => x+ ",zeyo")
					println("=====condata data==== ")
					condata.foreach(println)
					println
	}
}

