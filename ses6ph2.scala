/*
## Txn column data processing:-
## Dataframe conversion:-
--------------------------------------------------------------
For rdd every row is a element
Column based processing is possible by default
Mapsplit
define case class
impose case class
Filter with Column itself
Dataframe conversion ( schemardd.toDF() )
df.write.parquet("")
--------------------------------------------------------------------
*/
package pack
import org.apache.spark._
import org.apache.spark.sql.SparkSession

object ses6ph2{
  case class zeyoschema(id:String, category:String,product:String,mode:String)
  
	def main(args:Array[String]):Unit={
    
			    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					
					val data = sc.textFile("file:///C:/data/datatxns.txt")
					println("===raw data===")
					println
					data.foreach(println)
					println
					val gymdata = data.filter( x => x.contains("Gymnastics"))
					
					println("===Row gymdata ===")
					println
					gymdata.foreach(println)
					println

					val mapsplit = data.map( x => x.split(",")) 
					val schemardd = mapsplit.map( x => zeyoschema(x(0),x(1),x(2),x(3))) 
					val finalfilter = schemardd.filter( x   =>  x.product.contains("Gymnastics")) 
					
					println("===Column gymdata ===")
					finalfilter.foreach(println)
					println
					val df = finalfilter.toDF()
					println("==Dataframe ===")
					df.show()
	}
}
