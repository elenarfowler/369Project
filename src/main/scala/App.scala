
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val tvShows = Source.fromFile("netflixIMDB.csv").getLines.toList
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1).equals("TV Show"))
      .filter(line => !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4).equals("bam"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5).contains("United States"))
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(8)))
      .map(actorRating => actorRating._1.split(",").map(actor =>
        (actor.trim().replace("\"", ""), actorRating._2)))
      .map(x => x.foreach(println(_)))
  }
}