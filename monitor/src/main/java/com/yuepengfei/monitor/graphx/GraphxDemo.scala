package com.yuepengfei.monitor.graphx

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphxDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val v: RDD[(VertexId, String)] = sc.makeRDD(Array((1L,"Ann"),
      (2L,"Bill"),(3L,"Charles"),(4L,"Diane"),(5L,"Likes-status")))
    val e: RDD[Edge[String]] = sc.makeRDD(Array(Edge(1L,3L,"is-friends-with"),
      Edge(2L,3L,"is-friends-with"),Edge(3L,4L,"is-friends-with"),Edge(3L,5L,"Likes-status"),
      Edge(4L,5L,"Wroge-status")))
    val g: Graph[String, String] = Graph(v,e)

    // 计算连通体
    val components: Graph[VertexId, String] = g.connectedComponents()
    val vertices: VertexRDD[VertexId] = components.vertices
    //
    v join vertices map {case (id, (user, minId)) => (minId, user)} foreach println

  }
}
