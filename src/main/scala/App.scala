
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.pow

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

    // Getting the linear regression formula for predicting ratings
    val USMoviesandBudget = sc.textFile("archive/movies.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(16).contains("$"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).toInt >= 2000)
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(0).trim(),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(16).trim().replace("$", "")))

    val movieBudgetRatings = USMoviesandBudget.join(ratings)
      .map{case(k, (v1, v2)) => (v1.toFloat, v2.toFloat)}

    val size = movieBudgetRatings.keys.count()
    val budgetsMean = movieBudgetRatings.keys.sum()/size
    val ratingsMean = movieBudgetRatings.values.sum()/size

    val covariance = movieBudgetRatings
      .map{case(k, v) => (k - budgetsMean.toFloat) * (v - ratingsMean.toFloat)}.reduce((x,y) => x + y)

    val variance = movieBudgetRatings.keys.map(x => pow((x - budgetsMean), 2)).reduce((x,y) => x + y)

    val m = covariance/variance
    val b = ratingsMean - m * ratingsMean
    println(m + " " + b)
    // m = 1.2917395971535428E-8
    // b = 5.478972809028917
    // a movie with 125 million budget (HP and Sorcerer's Stone) would be predictoed to have a 7.08 rating (actual is 7.6)
    // a movie with 250 million budget (HP and the DH) would be predictoed to have a 8.699 rating (actual is 8.1)
    // a movie with 356 million budget (Endgame) would be predicted to have a 10.069 rating lol (actual is 8.4)
  }
}