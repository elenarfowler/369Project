
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.pow

object App {
  // statistics and stuff
  def averageBudgetRating(sparkContext: SparkContext): Unit = {
    //14=user rating, 16=budget 17=USgross 18=WorldGross (under movies)
    // setting up
    // rating, budget
    val USMoviesandBudgetRatings = sparkContext.textFile("archive/movies.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(16).contains("$"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).toInt >= 2000)
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(14).trim().toDouble,
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(16).trim().replace("$", "").toDouble))

    println("Average budgets for movies based on audience scores")
// highly rated movies over 8
    val over8 = USMoviesandBudgetRatings.filter(_._1 >= 8.0)
    val sizeOver8 = over8.count()
    val averageBudgetOver8 = over8.values.sum() / sizeOver8
    println("Budget for audience score over 8.0: " + averageBudgetOver8)
// movies with ratings between 6-8
    val btwn6n8 = USMoviesandBudgetRatings
      .filter(_._1 >= 6.0)
      .filter(_._1 < 8.0)
    val sizeBtwn6n8 = btwn6n8.count()
    val averageBudgetBtwn6n8 = btwn6n8.values.sum() / sizeBtwn6n8
    println("Budget for audience score between 6.0 and 8.0: " + averageBudgetBtwn6n8)
// movies with ratings between 4-6
    val btwn4n6 = USMoviesandBudgetRatings
      .filter(_._1 >= 4.0)
      .filter(_._1 < 6.0)
    val sizeBtwn4n6 = btwn4n6.count()
    val averageBudgetBtwn4n6 = btwn4n6.values.sum() / sizeBtwn4n6
    println("Budget for audience score between 4.0 and 6.0: " + averageBudgetBtwn4n6)

//movies with ratings under 4
    val under4= USMoviesandBudgetRatings
      .filter(_._1 < 4.0)
    val sizeUnder4 = under4.count()
    val averageBudgetUnder4 = under4.values.sum() / sizeUnder4
    println("Budget for audience score under 4.0: " + averageBudgetUnder4)
  }

  //find average worldwide income of a highly rated movie (8+) (profits)
  def averageIncomeHighRating(sparkContext: SparkContext): Unit = {
    val USMoviesandIncomeRatings = sparkContext.textFile("archive/movies.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(18).contains("$"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).toInt >= 2000)
//      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(14).toDouble >= 8.0)
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(14).trim().toDouble,
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(18).trim().replace("$", "").toDouble))

    println("Average global profits for movies based on audience scores")
    // highly rated movies over 8
    val over8 = USMoviesandIncomeRatings.filter(_._1 >= 8.0)
    val sizeOver8 = over8.count()
    val averageBudgetOver8 = over8.values.sum() / sizeOver8
    println("Global profit for audience score over 8.0: " + averageBudgetOver8)
    // movies with ratings between 6-8
    val btwn6n8 = USMoviesandIncomeRatings
      .filter(_._1 >= 6.0)
      .filter(_._1 < 8.0)
    val sizeBtwn6n8 = btwn6n8.count()
    val averageBudgetBtwn6n8 = btwn6n8.values.sum() / sizeBtwn6n8
    println("Global profit for audience score between 6.0 and 8.0: " + averageBudgetBtwn6n8)
    // movies with ratings between 4-6
    val btwn4n6 = USMoviesandIncomeRatings
      .filter(_._1 >= 4.0)
      .filter(_._1 < 6.0)
    val sizeBtwn4n6 = btwn4n6.count()
    val averageBudgetBtwn4n6 = btwn4n6.values.sum() / sizeBtwn4n6
    println("Global profit for audience score between 4.0 and 6.0: " + averageBudgetBtwn4n6)

    //movies with ratings under 4
    val under4= USMoviesandIncomeRatings
      .filter(_._1 < 4.0)
    val sizeUnder4 = under4.count()
    val averageBudgetUnder4 = under4.values.sum() / sizeUnder4
    println("Global profit for audience score under 4.0: " + averageBudgetUnder4)
  }

  // i realized idk what to do with this??
  def averageIncometoBudget(sparkContext: SparkContext): Unit ={
    val USMoviesandIncomeRatings = sparkContext.textFile("archive/movies.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(18).contains("$"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).toInt >= 2000)
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(16).trim().replace("$", "").toDouble,
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(18).trim().replace("$", "").toDouble))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("Lab6").setMaster("local[4]")
    val sc = new SparkContext(conf)
    averageIncomeHighRating(sc)

    println("tv actors:")

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
    println("movies actors:")

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

    println("movie directors: ")
    val movieDirectors = sc.textFile("netflixIMDB.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1).equals("Movie"))
      .filter(line => !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).equals(""))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5).contains("United States"))

      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3),
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(8)))
      .flatMap({case (key, value) => key.split(",")
        .map(director => (director.trim().replace("\"", ""), value.toDouble))})
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
    //grabs lines with movies from US that are made after 1999, with budget information
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
    // a movie with 125 million budget (HP and Sorcerer's Stone) would be predicted to have a 7.08 rating (actual is 7.6)
    // a movie with 250 million budget (HP and the DH) would be predicted to have a 8.699 rating (actual is 8.1)
    // a movie with 356 million budget (Endgame) would be predicted to have a 10.069 rating lol (actual is 8.4)
  }
}