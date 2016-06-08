package com.cocycles.challenge

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.reflect.ClassTag

/**
 * this is a small ex given out by a small company with a big idea
 **/
object GraphXDependencyMapper {

  def createSparkContext() = {
    val sparkConf = new SparkConf().setAppName("UberJob")
    new SparkContext(sparkConf)
  }
  import org.apache.spark.graphx._
  val VD = Set[Long]()
  def main(args: Array[String]): Unit = {
    val sc = createSparkContext()
    val matrix: org.apache.spark.rdd.RDD[(Long, List[Long])] = sc.objectFile("/matrix.txt")
    val edges = matrix.flatMap { case (v, es) => es.map(Edge(v, _, "dependsOn")) } // generate Edges from matrix file
    val g = Graph.fromEdges(edges, VD).reverse // generate graph. Vertex name is meaningless. Reverse so that dependencies are accumulated at the depending module instead of at the bottom-most component in the connected component subgraph
    val t1 = g.pregel(VD)((id, d, msg) =>  d ++ msg, // iterate over graph till no more nodes left to add dependencies
      t => {
        if(t.dstAttr(t.dstId)) Iterator.empty
        else if(t.dstAttr(t.srcId) && (t.srcAttr subsetOf t.dstAttr)) Iterator.empty
        else Iterator((t.dstId, t.srcAttr + t.srcId))
      }, (a, b) => a ++ b)

    val result = t1.vertices.map(v=>(v._1, v._2.toList))
  }


}

