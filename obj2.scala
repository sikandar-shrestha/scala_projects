package pack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object obj2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("First").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///C:/data/scdata.txt")

    println("===== Raw Rdd=======")
    println
    data.foreach(println)
    println

    val flatdata = data.flatMap(x => x.split("~"))

    println("===== flatdata Rdd=======")
    println
    flatdata.foreach(println)
    println

    val statedata = flatdata.filter(x => x.toLowerCase().contains("state"))

    println("===== statedata Rdd=======")
    println
    statedata.foreach(println)
    println

    val citydata = flatdata.filter(x => x.contains("City"))

    println("===== citydata Rdd=======")
    println
    citydata.foreach(println)
    println

    val finalstate = statedata.map(x => x.replace("State->", ""))

    println("===== finalstate Rdd=======")
    println
    finalstate.foreach(println)
    println

    val finalcity = citydata.map(x => x.replace("City->", ""))

    println("===== finalcity Rdd=======")
    println
    finalcity.foreach(println)
    println

    //finalstate.coalesce(1).saveAsTextFile("file:///C:\data\statedata")

  }

}