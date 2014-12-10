import org.apache.spark.SparkContext
import org.sparkalgos.mllib.join.KnnJoin

object Knn {

   def main (args: Array[String]) {

     /*val model = knnJoin.knnJoin(dataset : RDD[Vector[Int]],
       datapoint : Vector[Int], len : Int, iteration : Int)*/
     val sc = new SparkContext("local","knn")

     val vectors = Seq(
        Vector(0, 0, 0),
        Vector(1, 2, 3),
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

     val model = KnnJoin.knnJoin( data, point, len, iter)

     model.saveAsTextFile("/home/ashu/Desktop/knn")

   }

}
