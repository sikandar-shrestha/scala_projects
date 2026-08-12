package pack
import org.apache.spark._

object obj1 {

  def main(args: Array[String]): Unit = {

    //Filter elements Contains "india"
    //From that result Flatten with -
    //From that Result replace "india" with "local"
    //From that Result contact ",zeyo"

    println("========Started=======")

    val liststr = List(
      "Amazon-Jeff-America",
      "Microsoft-BillGates-America",
      "TCS-TATA-india",
      "Reliance-Ambani-india")

    println("=====raw List====")

    liststr.foreach(println)

    val filstr = liststr.filter(x => x.contains("india"))

    println("=====filter List====")

    filstr.foreach(println)

    println("=====flatmap List====")

    val flatdata = filstr.flatMap(x => x.split("-"))

    flatdata.foreach(println)

    println("=====replace List====")

    val repdata = flatdata.map(x => x.replace("india", "local"))

    repdata.foreach(println)

    println("===concat list =====")

    val mapdata = repdata.map(x => x.concat(",sai"))

    mapdata.foreach(println)

  }

}