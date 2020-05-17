package sparkml.basicStatistics

import com.yuepengfei.monitor.sparkstreaming.SparkTrait

object HypothesisDemo extends App with SparkTrait{
  import org.apache.spark.ml.linalg.{Vector, Vectors}
  import org.apache.spark.ml.stat.ChiSquareTest
  import spark.implicits._

  val data = Seq(
    (0.0, Vectors.dense(0.5, 10.0)),
    (0.0, Vectors.dense(1.5, 20.0)),
    (1.0, Vectors.dense(1.5, 30.0)),
    (0.0, Vectors.dense(3.5, 30.0)),
    (0.0, Vectors.dense(3.5, 40.0)),
    (1.0, Vectors.dense(3.5, 40.0))
  )

  /**
   * 假设检验得到的都是什么结果？？？
   */
  val df = data.toDF("label", "features")
  val chi = ChiSquareTest.test(df, "features", "label").head
  println(s"pValues = ${chi.getAs[Vector](0)}")
  println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
  println(s"statistics ${chi.getAs[Vector](2)}")

  spark.stop()
}
