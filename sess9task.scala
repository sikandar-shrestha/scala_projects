package pack

import org.apache.spark.SparkContext  // rdd
import org.apache.spark.sql.SparkSession  // dataframe
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object sess9task {


	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setMaster("local[*]").setAppName("first")

			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()
			import spark.implicits._

			val df = spark
				  .read
				  .format("xml")
			        .option("rowtag","POSLog")
			        .load("file:///C:/data/transactions.xml")
					
			df.show()
			
			df.printSchema()
					
					
	}
}