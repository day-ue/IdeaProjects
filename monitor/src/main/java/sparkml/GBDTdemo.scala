package sparkml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object GBDTdemo extends App {
  val conf=new SparkConf().setAppName("GBDTExample").setMaster("local[*]")
  val sc=new SparkContext(conf)

  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  import spark.implicits._

  val data: DataFrame = spark.read.format("libsvm").load("./data/spark/sample_libsvm_data.txt")

  val splits=data.randomSplit(Array(0.8,0.2))

  val (trainData,testData)=(splits(0),splits(1))

  val labelIndexer = new StringIndexer()
    .setInputCol("label")
    .setOutputCol("indexLable")
    .fit(data)

  val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4)
    .fit(data)

  val gbdt=new GBTClassifier()
    .setLabelCol("indexLable")
    .setFeaturesCol("indexedFeatures")
    .setMaxIter(10)

  val lableConvert=new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictionLable")
    .setLabels(labelIndexer.labels)

  val pipeline=new Pipeline()
    .setStages(Array(labelIndexer,featureIndexer,gbdt,lableConvert))

  val model=pipeline.fit(trainData)

  val predications=model.transform(testData)

  predications.select("predictionLable", "label", "features").show(100)

  val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]

  println("Learned classification GBT model:\n" + gbtModel.toDebugString)

}
