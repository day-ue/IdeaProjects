package sparkml.basicStatistics
import com.yuepengfei.monitor.sparkstreaming.sparkTrait
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row

object CorrelationDemo  extends App with sparkTrait{

  import spark.implicits._

  /**
   * vectors表示向量
   * dense稠密向量：【1,0,2,3】
   * sparse稀疏矩阵：向量长度，索引数组，与索引数组所对应的数值数组
   *                向量长度，（索引，数值），（索引，数值），（索引，数值），...(索引，数值)
   */

  val data = Seq(
    Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
  )

  /**
   *
   */
  val df = data.map(Tuple1.apply).toDF("features")
  val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
  println(s"Pearson correlation matrix:\n $coeff1")

  val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
  println(s"Spearman correlation matrix:\n $coeff2")

  spark.stop()


}
