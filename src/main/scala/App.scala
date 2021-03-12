
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
      .filter(line => line.length > 2 && line.substring(0, 2).equals("nm"))
      .map(line => (line.split(",")(0), line.split(",")(1)))

    val movieRatings = USMoviesandGenre.join(ratings).map{case(k, (_, v2)) => (k,v2)}

    val actorRatings = movieRatings.join(movieActors).map{case(k, (v1, v2)) => (v2, v1.toDouble)}
      .combineByKey(v => (v, 1.0),
      (acc: (Double, Double), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Double, Double), acc2: (Double, Double)) => (acc1._1 + acc2._1,
        acc1._2 + acc2._2))
      .map { case (key, value) => (key, value._1 * 1.0 / value._2) }
      .join(names)
      .map({case (key, value) => (value._2, value._1)})
      .sortBy(x => x._2,false)
      .take(10).foreach(println(_))
  }
}