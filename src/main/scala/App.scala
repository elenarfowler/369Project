
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val USMoviesandGenre = sc.textFile("archive/movies.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(0),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5)))

    val ratings = sc.textFile("archive/ratings.csv")
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(0),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1)))

    val movieActors = sc.textFile("archive/title_principals.csv")
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(0),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(2)))

    val names = sc.textFile("archive/names.csv")
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(0),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1)))

    val movieRatings = USMoviesandGenre.join(ratings).map{case(k, (_, v2)) => (k,v2)}

    val actorRatings = movieRatings.join(movieActors).map{case(k, (v1, v2)) => (v2, v1.toDouble)}
      .groupByKey().map({case (key, value) => (key, value.aggregate((0.0, 0.0))(
      (x,y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2)
    ))}).map({case (key, value) => (value._1/value._2, key)})
      .sortByKey(false).collect().map({case (key, value) => (value, key)})
      .take(10).foreach(println(_))

//    val tvShows = sc.textFile("netflixIMDB.csv")
//      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1).equals("TV Show"))
//      .filter(line => !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4).equals("bam"))
//      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5).contains("United States"))
//      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4),
//        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(8)))
//      .flatMap({case (key, value) => key.split(",")
//        .map(actor => (actor.trim().replace("\"", ""), value.toDouble))})
//      .groupByKey()
//      .map({case (key, value) => (key, value.aggregate((0.0, 0.0))(
//        (x,y) => (x._1 + y, x._2 + 1),
//        (x,y) => (x._1 + y._1, x._2 + y._2)
//      ))})
//      .map({case (key, value) => (value._1/value._2, key)})
//      .sortByKey(false)
//      .collect()
//      .map({case (key, value) => (value, key)})
//      .take(10)
//      .foreach(println(_))
//
//    val movie = sc.textFile("netflixIMDB.csv")
//      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1).equals("Movie"))
//      .filter(line => !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4).equals("bam"))
//      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5).contains("United States"))
//
//      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(4),
//        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(8)))
//      .flatMap({case (key, value) => key.split(",")
//        .map(actor => (actor.trim().replace("\"", ""), value.toDouble))})
//      .groupByKey()
//      .map({case (key, value) => (key, value.aggregate((0.0, 0.0))(
//        (x,y) => (x._1 + y, x._2 + 1),
//        (x,y) => (x._1 + y._1, x._2 + y._2)
//      ))})
//      .map({case (key, value) => (value._1/value._2, key)})
//      .sortByKey(false)
//      .collect()
//      .map({case (key, value) => (value, key)})
//      .take(10)
//      .foreach(println(_))
  }
}