import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("DisneylandReviewsAnalysis")
      .master("local")
      .getOrCreate()

    val reviewsDf = ReadCSV.readData(spark) // Jeu de Données initial
    val cleanedDf = DataCleaner.cleanData(reviewsDf)  //Jeu de données traités
    DataExplorer.exploreData(cleanedDf)

    // Création d'une vue temporaire pour exécuter des requêtes SQL
    cleanedDf.createOrReplaceTempView("disneyland_reviews")
      //reviewsDf.createOrReplaceTempView("disneyland_reviews")
    // Par exemple, pour compter les valeurs "missing" dans Year_Month
    val missingCount = spark.sql("""
      SELECT COUNT(*) as MissingCount
      FROM disneyland_reviews
      WHERE Year_Month = 'missing'
    """)
    missingCount.show()

    val rating = spark.sql("""
      SELECT Rating, COUNT(*) as TotalReviews
      FROM disneyland_reviews
     GROUP BY Rating
      ORDER BY Rating
     """)
      rating.show()

    val reviewsCountry = spark.sql("""
      SELECT Reviewer_Location, COUNT(*) as TotalReviews
      FROM disneyland_reviews
      GROUP BY Reviewer_Location
      ORDER BY TotalReviews DESC
      """)
      reviewsCountry.show()

    val reviewsPark = spark.sql("""
      SELECT Branch, COUNT(*) as TotalReviews
      FROM disneyland_reviews
      GROUP BY Branch
      ORDER BY TotalReviews DESC
""")
      reviewsPark.show()


    // Fermer la session Spark à la fin
    spark.stop()
  }
}