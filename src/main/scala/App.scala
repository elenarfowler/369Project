
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.pow

object App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setAppName("369Project").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val ratings = sc.textFile("archive/ratings.csv")
    .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(0),
      line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1)))
    .persist()

  val movies = sc.textFile("archive/movies.csv")
    .filter(line => ( !line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).equals("year") &&
      line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).length == 4 &&
      line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).toInt >= 2000) &&
      line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
    .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(0),
      line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(12)))
    .join(ratings)
    .persist()

  val USMoviesandGenre = sc.textFile("archive/movies.csv")
    .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
    .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(0),
      line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(5)))

  // statistics and stuff
  def averageBudgetRating(): Unit = {
    //14=user rating, 16=budget 17=USgross 18=WorldGross (under movies)
    // setting up
    // rating, budget
    val USMoviesandBudgetRatings = sc.textFile("archive/movies.csv")
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
  //14 = rating 18 = world income
  def averageIncomeHighRating(): Unit = {
    val USMoviesandIncomeRatings = sc.textFile("archive/movies.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(18).contains("$"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).toInt >= 2000)
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
  //find average rating based on income
  //14 = rating 18 = world income
  def averageRatingBasedOnIncome(): Unit = {
    val USMoviesandIncomeRatings = sc.textFile("archive/movies.csv")
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(7).contains("USA"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(18).contains("$"))
      .filter(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(3).toInt >= 2000)
      .map(line => (line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(14).trim().toDouble,
        line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(18).trim().replace("$", "").toDouble))

    println("Average rating for movies based on profit")
    // avg rating over 100,000,000 (100 million)
    val over8 = USMoviesandIncomeRatings.filter(_._2 >= 100000000)
    val sizeOver8 = over8.count()
    val averageBudgetOver8 = over8.keys.sum() / sizeOver8
    println("Average rating for profit over $100,000,000: " + averageBudgetOver8)
    // avg rating btwn 50,000,000-100,000,000 (between 50mil and 100mil)
    val btwn6n8 = USMoviesandIncomeRatings
      .filter(_._2 >= 50000000)
      .filter(_._2 < 100000000)
    val sizeBtwn6n8 = btwn6n8.count()
    val averageBudgetBtwn6n8 = btwn6n8.keys.sum() / sizeBtwn6n8
    println("Average rating for profit between $50,000,000 - $100,000,000: " + averageBudgetBtwn6n8)
//    avg rating btwn 10,000,000-50,000,000 (between 10mil and 50mil)
    val btwn4n6 = USMoviesandIncomeRatings
      .filter(_._2 >= 10000000)
      .filter(_._2 < 50000000)
    val sizeBtwn4n6 = btwn4n6.count()
    val averageBudgetBtwn4n6 = btwn4n6.keys.sum() / sizeBtwn4n6
    println("Average rating for profit between $10,000,000 - $50,000,000: " + averageBudgetBtwn4n6)
    //  avg rating btwn 500,000 - 10,000,000 (between 500k and 10mil)
    val under4= USMoviesandIncomeRatings
        .filter(_._2 >= 500000)
      .filter(_._2 < 10000000)

    val sizeUnder4 = under4.count()
    val averageBudgetUnder4 = under4.keys.sum() / sizeUnder4
    println("Average rating for profit between $500,000 - $10,000,000: " + averageBudgetUnder4)

    // avg rating under 500k
    val under500= USMoviesandIncomeRatings
      .filter(_._2 < 500000)

    val sizeUnder500 = under500.count()
    val averageBudgetUnder500 = under500.keys.sum() / sizeUnder500
    println("Average rating for profit under $500k: " + averageBudgetUnder500)

  }


  def predictingRatingsFromBudget(): Unit = {
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
    // a movie with 125 million budget (HP and Sorcerer's Stone) would be predicted to have a 7.08 rating (actual is 7.6)
    // a movie with 250 million budget (HP and the DH) would be predicted to have a 8.699 rating (actual is 8.1)
    // a movie with 356 million budget (Endgame) would be predicted to have a 10.069 rating lol (actual is 8.4)

    val movieIDBudgetRatings = USMoviesandBudget.join(ratings)
    val predictions = movieIDBudgetRatings.map{case(k, (budget,_)) => (k, m*budget.toFloat + b)}
    val sum_error = movieIDBudgetRatings.map{case(k, (_,rating)) => (k, rating)}.join(predictions)
      .map{case(k, (actual, prediction)) => (actual.toFloat, prediction)}.map(x => x._2 - x._1).sum()
    val rmse = Math.sqrt(sum_error/size)
    println(rmse)

    // RMSE = 0.5250978331346486
  }

  def findingTopRatedActors(): Unit = {
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



  //takes about 7 min for 1500 test movies for reference
  def testRatingByActorsPredictions(numTestRecords: Int): Unit = {
    val result = movies.take(numTestRecords)
      .map({case (key, value) =>
        if(value._1.length >0) {
          (key, value._2 + ", " + predictRatingByActors(value._1.substring(1, value._1.length-1).split(", "), key))
        } else {
          (key, value._2 + ", " + predictRatingByActors(value._1.split(", "), key))
        }})

    val calculated = result.filter({case (key, value) => value.split(", ")(1).toDouble != 0.0})

    val withinOne = calculated
      .filter({case (key, value) => (1 >= (value.split(", ")(0).toDouble - value.split(", ")(1).toDouble).abs)})

    println("Number of Input Movies: " + result.size)
    println("Number of Ratings Predicted: " + calculated.size)
    println("Number of Predictions within 1 Point: " + withinOne.size)
    println("Percentage of Predictions within 1 Point: " + (1.0* withinOne.size) / calculated.size)
  }

  def predictRatingByActors(actors: Array[String], id: String = null): Double = {
    val result = movies
      .filter({case (key, value) => !(id != null && key.equals(id))})
      .map({case (key, value) =>
        if(value._1.length >0) {
          (value._2, value._1.substring(1, value._1.length-1).split(", ").intersect(actors).size)
        } else {
          (value._2, value._1.split(", ").intersect(actors).size)
        }})
      .sortBy({case (key, value) => value}, false)
      .take(10)
      .filter({case (key, value) => value!=0})
      .map({case (key, value) => key.toDouble})
      .aggregate((0.0, 0.0))(
        (x,y) => (x._1 + y, x._2 + 1),
        (x,y) => (x._1 + y._1, x._2 + y._2)
      )
    if(result._2 == 0) {
      return 0;
    }
    result._1*1.0/result._2
  }

  //use to see which movies were used to determine prediction for a given list of actors
  def predictRatingByActorsDetailed(actors: Array[String], id: String = null): Array[(Double, String)] = {
    val result = movies
      .filter({case (key, value) => !(id != null && key.equals(id))})
      .map({case (key, value) =>
        if(value._1.length >0) {
          (value._2, value._1.substring(1, value._1.length-1).split(", ").intersect(actors).size + ", " + value._1)
        } else {
          (value._2, value._1.split(", ").intersect(actors).size + ", " + value._1)
        }})
      .sortBy({case (key, value) => value.split(", ")(0).toInt}, false)
      .take(10)
      .filter({case (key, value) => value.split(", ")(0).toInt!=0})

    val r = result
      .map({case (key, value) => key.toDouble})
      .aggregate((0.0, 0.0))(
        (x,y) => (x._1 + y, x._2 + 1),
        (x,y) => (x._1 + y._1, x._2 + y._2)
      )
    val avg = r._1*1.0/r._2
    result.map({case (key, value) => (avg, value)})
  }
  def main(args: Array[String]): Unit = {

    //averageIncomeHighRating(sc)
    // Global profit for audience score over 8.0: 3.907898231388889E8
    // Global profit for audience score between 6.0 and 8.0: 8.452461252132949E7
    // Global profit for audience score between 4.0 and 6.0: 2.903742218363764E7
    // Global profit for audience score under 4.0: 8560361.581460673
    averageBudgetRating()
//    averageRatingBasedOnIncome()
    //    predictingRatingsFromBudget()
    //    findingTopRatedActors()
  }
}