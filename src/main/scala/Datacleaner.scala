import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataCleaner {
  def cleanData(df: DataFrame): DataFrame = {
    // Ajouter un zéro devant les mois à un seul chiffre (ex: '2019-4' devient '2019-04')
    val correctDateFormat = udf((yearMonth: String) => {
      if (yearMonth != null && yearMonth.matches("\\d{4}-\\d{1}")) {
        yearMonth.substring(0, 5) + "0" + yearMonth.substring(5)
      } else {
        yearMonth
      }
    })

    val dfFormatted = df.withColumn("Year_Month", correctDateFormat(col("Year_Month")))

    // Convertir Year_Month en un format de date, si possible
    val dfWithDate = dfFormatted.withColumn("Year_Month", to_date(col("Year_Month"), "yyyy-MM"))

    // Liste des noms de parcs valides pour Disneyland_Branch
    val validBranchNames = Seq("Disneyland_Paris", "Disneyland_California", "Disneyland_HongKong")

    // Remplacer les valeurs invalides dans Disneyland_Branch
    val dfCleanedBranch = dfWithDate.withColumn("Branch",
      when(col("Branch").isin(validBranchNames: _*), col("Branch"))
        .otherwise(lit("Unknown")))

    dfCleanedBranch
  }
}


