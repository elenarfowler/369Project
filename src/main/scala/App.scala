
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val tvActors = mutable.ListBuffer[String]()
    val tvActorsRating = mutable.Map[String, (Double, Int)]()

    val tvShows = Source.fromFile("netflixIMDB.csv").getLines.toList
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1).equals("TV Show"))
      .filter(line => !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4).equals("bam"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5).contains("United States"))
      .foreach(line =>
        {
          val rating = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(8)
          line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4).split(",")
            .foreach(actor => tvActors += actor.trim().replace("\"", "") + "," + rating)
        })

    tvActors.foreach(actorRating =>
      if (tvActorsRating.contains(actorRating.split(",")(0)))
        tvActorsRating(actorRating.split(",")(0)) = (actorRating.split(",")(1).toDouble
          + tvActorsRating(actorRating.split(",")(0))._1, tvActorsRating(actorRating.split(",")(0))._2 + 1)
      else
        tvActorsRating.put(actorRating.split(",")(0), (actorRating.split(",")(1).toDouble, 1))
    )

    // prints (name, (avgrating, # of shows))
    tvActorsRating.map(x => (x._1, (x._2._1/x._2._2, x._2._2))).foreach(println(_))



  }
}