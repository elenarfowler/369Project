
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val tvShows = sc.textFile("netflixIMDB.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1).equals("TV Show"))
      .filter(line => !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4).equals("bam"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5).contains("United States"))
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(8)))
      .flatMap({case (key, value) => key.split(",")
        .map(actor => (actor.trim().replace("\"", ""), value.toDouble))})
      .groupByKey()
      .map({case (key, value) => (key, value.aggregate((0.0, 0.0))(
        (x,y) => (x._1 + y, x._2 + 1),
        (x,y) => (x._1 + y._1, x._2 + y._2)
      ))})
      .map({case (key, value) => (value._1/value._2, key)})
      .sortByKey(false)
      .collect()
      .map({case (key, value) => (value, key)})
      .take(10)
      .foreach(println(_))

    val movie = sc.textFile("netflixIMDB.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1).equals("Movie"))
      .filter(line => !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4).equals("bam"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5).contains("United States"))

      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(8)))
      .flatMap({case (key, value) => key.split(",")
        .map(actor => (actor.trim().replace("\"", ""), value.toDouble))})
      .groupByKey()
      .map({case (key, value) => (key, value.aggregate((0.0, 0.0))(
        (x,y) => (x._1 + y, x._2 + 1),
        (x,y) => (x._1 + y._1, x._2 + y._2)
      ))})
      .map({case (key, value) => (value._1/value._2, key)})
      .sortByKey(false)
      .collect()
      .map({case (key, value) => (value, key)})
      .take(10)
      .foreach(println(_))
  }
}