package pack

import org.apache.spark.SparkContext  // rdd
import org.apache.spark.sql.SparkSession  // dataframe
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object partitionBy {


	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setMaster("local[*]").setAppName("first")



					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")


					val spark = SparkSession.builder.config(conf).getOrCreate()

					import spark.implicits._

					val dschema = StructType(Array(
							StructField("id",StringType,true),
							StructField("name",StringType,true),
							StructField("chk",StringType,true),
							StructField("country", StringType, true)
							))


					val df = spark
					.read
					.format("csv")
					.schema(dschema)
					.load("file:///C:/data/allcountry1.csv")

					df.show() 
					
					
					
					df.write
					      .format("csv")
					      .partitionBy("country")
					      .mode("overwrite")
					      .save("file:///C:/data/countrypart")
			
	}
}