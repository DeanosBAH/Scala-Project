
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ML {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("SentimentAnalysis")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val reviewsDf = ReadCSV.readData(spark) // Jeu de Données initial
    val data = DataCleaner.cleanData(reviewsDf) //Jeu de données déjà nettoyés


    //Convertir les scores en des libellés catégoriques
    val toLabel = udf((rating: Int) => rating match {
      case r if r >= 4 => "Positive"
      case r if r <= 2 => "Negative"
      case _ => "Neutral"
    })

    val labeledData = data.withColumn("label", toLabel($"Rating"))

    // Les éléments du process Pipeline
    // tokenizer: Couper le texte en mots
    val tokenizer = new RegexTokenizer().setInputCol("Review_Text").setOutputCol("Words").setPattern("\\W")

    // Supprimer les mots-clés non significatifs comme "est","la","vous"...
    val remover = new StopWordsRemover().setInputCol("Words").setOutputCol("FilteredWords")
    // la fréquence de chaque mot par score
    //Exemple : "Rating"=5 , le mot "Super" se répète 6 fois donc comme il se répète autant de fois en Positive il est considéré comme positive
    val vectorizer = new CountVectorizer().setInputCol("FilteredWords").setOutputCol("RawFeatures")
    //Réduire l'impact des mots qui apparaissent très fréquemment dans le texte et qui sont donc moins informatifs que les mots qui apparaissent moins fréquemment.
    val idf = new IDF().setInputCol("RawFeatures").setOutputCol("Features")
    //Indexer les libellés en des valeurs numériques ===> Entraîner le modèle de ML
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(labeledData)

    // Choix du modèle : Régression linéaire
    val lr = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("Features")

    // Reconvertir les indices en libellés
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labelsArray(0))

    // Définire le pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, remover, vectorizer, idf, labelIndexer, lr, labelConverter))

    // Diviser les données en 2 data set : Train data et Test Data
    val Array(trainingData, testData) = labeledData.randomSplit(Array(0.7, 0.3))

    // Entraîner le modèle
    val model = pipeline.fit(trainingData)

    // Prédictions
    val predictions = model.transform(testData)


    predictions.select("Review_Text", "label", "predictedLabel").show()

    // Evaluer le modèle
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    println(s"Accuracy = $accuracy")

    spark.stop()
  }
}
