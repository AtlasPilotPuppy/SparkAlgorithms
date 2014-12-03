package watershed_pregel

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection._
import scala.collection.mutable.Seq
import scala.util.Random
import scala.math._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object test {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local","pregelWatershed");
    val path1 = "/home/kaushik/Desktop/data.csv"
      
    val model = pregel_watershed.connectedComponents_to_point(sc, path1, 2L, 2L)
    model.saveAsTextFile("/home/kaushik/Desktop/result")
    
  }

}