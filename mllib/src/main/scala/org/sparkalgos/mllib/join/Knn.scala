/**
 * Created by ashu on 11/20/14.
 */

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
object Knn {

   def main (args: Array[String]) {

     /*val model = knnJoin.knnJoin(dataset : RDD[Vector[Int]],
       datapoint : Vector[Int], len : Int, iteration : Int, sc : SparkContext)*/
     val sc = new SparkContext("local","knn")

     val vectors = Seq(
        Vector(0, 0, 0),
        Vector(1,2, 3),
        Vector(1, 5, 4),
        Vector(5, 5, 8),
        Vector(1, 1, 2),
        Vector(1, 2, 4),
        Vector(3, 4, 5)
       )
     val data = sc.parallelize(vectors, 2)

     val point = Vector(1,5,3)
     val len = 3
     val iter = 4

     val model = knnJoin.knnJoin( data, point, len, iter, sc)

     model.saveAsTextFile("/home/ashu/Desktop/knn")

   }

}
