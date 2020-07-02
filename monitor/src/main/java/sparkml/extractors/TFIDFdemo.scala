package sparkml.extractors

import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, StopWordsRemover}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql.EsSparkSQL

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.linalg.Vector

object TFIDFdemo extends App {

  private val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
  private val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .config("es.nodes", "192.168.124.131")
    .config("es.port", "9200")
    .config("es.nodes.wan.only", "true")
    .getOrCreate()

  val query =
    """
      |{
      |  "query": {
      |    "match_all": {}
      |  }
      |}
      |""".stripMargin


  import spark.implicits._
  private val df: DataFrame = EsSparkSQL.esDF(spark.sqlContext, "my_document/doc", query).cache()


  val remover: StopWordsRemover = new StopWordsRemover()
    .setInputCol("words")
    .setOutputCol("filtered")

  remover.setStopWords(Array(")", "(", "年", "月", "日", "总", "键"))

  val segmenter: JiebaSegmenter = new JiebaSegmenter()
  val wordsData = df.select("fileNameHashCode", "document").rdd.map(row => {
    val fileNameHashCode: String = row.getAs[String]("fileNameHashCode")
    val document: String = row.getAs[String]("document")
      .replaceAll("[^(\\u4e00-\\u9fa5)]", "")
    val words = segmenter.process(document, SegMode.INDEX).asScala.map(x=>x.word)
    (fileNameHashCode, words)
  }).toDF("fileNameHashCode", "words")

  val removerWordsData = remover.transform(wordsData)

  // fit a CountVectorizerModel from the corpus
  val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("filtered")
    .setOutputCol("rawFeatures")
    .setVocabSize(10000)
    .setMinDF(2)
    .fit(removerWordsData)
  val featurizedData: DataFrame = cvModel.transform(removerWordsData)


  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val idfModel = idf.fit(featurizedData)
  val rescaledData: DataFrame = idfModel.transform(featurizedData)


  val voc: Array[String] = cvModel.vocabulary
  val getKeyWordsFun = udf((fea: Vector) =>{
    var arrw = ArrayBuffer[String]()
    var arrv = ArrayBuffer[Double]()
    fea.foreachActive((index: Int, value: Double) =>{
      arrw += voc(index)
      arrv += value
    })
    (arrw zip arrv).toList.sortBy(-_._2).take(10).map(x => x._1).toArray
  })

  val keywords: DataFrame = rescaledData
    .withColumn("keyword", getKeyWordsFun(col("features")))
    .select("fileNameHashCode", "keyword")

  val result: DataFrame = df.join(keywords, Seq("fileNameHashCode"), "left")
    .select("fileNameHashCode", "fileName", "keyword", "document")

  EsSparkSQL.saveToEs(result, "my_document_keywords/doc", Map("es.mapping.id" -> "fileNameHashCode"))

  spark.stop()

}
