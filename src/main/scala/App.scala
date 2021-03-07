
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col, concat_ws}

import scala.io.Source

object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // needed to parse as a dataframe first so that columns with commas didn't get separated and mess up indexing
//    val sqlContext = new SQLContext(sc)
//    val df = sqlContext.read.format("csv").option("header", "true").load("netflixIMDB.csv")
//    val selection = df.columns.map(col)
//    val tsv = df.select(concat_ws("\t", selection:_*))
//    val newData = tsv.rdd.map(_.toString().replace("[", "").replace("]", ""))


    //    val tvShows = newData.take(2000)
    //      .filter(line => line.split("\t")(1) == "TV Show" && line.split("\t")(4) != "bam")
    //      .filter(line => line.split("\t")(5).contains("United")) // not sure why its not getting all the United States ones
    //      .foreach(println(_))

    val tvShows = Source.fromFile("netflixIMDB.csv").getLines.toList
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1).equals("TV Show"))
      .filter(line => !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4).equals("bam"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5).contains("United States"))
      .foreach(println(_))



  }
}