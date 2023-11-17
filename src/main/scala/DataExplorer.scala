import org.apache.spark.sql.DataFrame

object DataExplorer {
  def exploreData(df: DataFrame): Unit = {
    println("First Few Rows:")
    df.show(20)

    println("Descriptive Statistics:")
    df.describe().show()

    println("Schema:")
    df.printSchema()
  }


}
