package sparkml

import com.yuepengfei.monitor.sparkstreaming.SparkTrait
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.Row


/**
 * pValues：评测值，越大【接近1】代表该特征列越无意义，对标签的区分作用越低，反之，越小【接近0】越有区分价值。
 * degreeOfFreedom：自由度，degreeOfFreedom+1等价于该特征值的种类。
 * statistics：处理逻辑比较复杂，可以认为是越大分类价值越高，越小分类价值越低。
 */


object ChiSquareTestScala extends App with SparkTrait{

  import spark.implicits._

  val data = Seq(
    (0.0, Vectors.dense(0.5, 10.0)),
    (1.0, Vectors.dense(1.5, 20.0)),
    (1.0, Vectors.dense(1.5, 30.0)),
    (1.0, Vectors.dense(3.5, 30.0)),
    (1.0, Vectors.dense(3.5, 40.0)),
    (1.0, Vectors.dense(3.5, 40.0))
  )

  val df = data.toDF("label", "features")
  val chi: Row = ChiSquareTest.test(df, "features", "label").head
  println(s"pValues = ${chi.getAs[Vector](0)}")
  println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
  println(s"statistics ${chi.getAs[Vector](2)}")

  ChiSquareTest.test(df, "features", "label").show()


}
