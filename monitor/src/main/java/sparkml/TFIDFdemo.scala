package sparkml
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession
object TFIDFdemo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TFIDFdemo")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  val sentenceData = spark.createDataFrame(Seq(
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
  )).toDF("label", "sentence")

  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
  val wordsData = tokenizer.transform(sentenceData)

  val hashingTF = new HashingTF()
    .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

  val featurizedData = hashingTF.transform(wordsData)
  // alternatively, CountVectorizer can also be used to get term frequency vectors

  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val idfModel = idf.fit(featurizedData)

  val rescaledData = idfModel.transform(featurizedData)
  rescaledData.select("label", "features").foreach(x=>{
    println(x)
  })

  spark.stop()

}
