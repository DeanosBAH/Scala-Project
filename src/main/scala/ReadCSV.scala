import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadCSV {
  def readData(spark: SparkSession): DataFrame = {

    spark.read.option("header", "true").csv("src/main/Ressource/DisneylandReviews.csv")
  }
}
