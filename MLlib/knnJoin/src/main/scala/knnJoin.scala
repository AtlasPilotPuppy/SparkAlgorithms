import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import scala.util.Random

object knnJoin {

  /**
   * Computes the nearest neighbors in the data-set for the data-point against which KNN
   * has to be applied for A SINGLE ITERATION
   *
   * @param rdd : RDD of Vectors of Int, which is the data-set in which knnJoin has to be undertaken
   * @param dataPoint : Vector of Int, which is the data-point with which knnJoin is done with the data-set
   * @param randPoint : Vector of Int, it's the random vector generated in each iteration
   * @param len : the number of data-points from the data-set on which knnJoin is to be done
   * @param zScore : RDD of (Long,Long), which is the ( <line_no> , <zscore> ) for each entry of the dataset
   * @param dataScore : Long value of z-score of the data-point
   * @return an RDD of the nearest 2*len entries from the data-point on which KNN needs to be undertaken for that iteration
   */
  def knnJoin_perIteration(rdd : RDD[(Vector[Int],Long)],
                           dataPoint : Vector[Int],
                           randPoint : Vector[Int],
                           len : Int,
                           zScore : RDD[(Long,Long)],
                           dataScore : Long,
                           sc : SparkContext) : RDD[(Vector[Int],Long)] = {


   // rdd with score greater than the z-score of the data-point
   val greaterRDD = zScore.filter(word  => word._2 > dataScore).
            map(word => word._2 -> word._1).
            sortByKey(true).
            map(word => word._2).
            zipWithIndex()
   // rdd with score lesser than the z-score of the data-point
   val lesserRDD = zScore.filter(word => word._2 < dataScore)
                .map(word => word._2 -> word._1)
                .sortByKey(false)
                .map(word => word._2)
                .zipWithIndex()


   /**
    * Need 2*len entries, hence the IF-ELSE construct to guarantee these many no.of entries in
    * the returned RDD
    * if the no.of entries in the greaterRDD and lesserRDD is greater than <len>
    * extract <len> no.of entries from each RDD
    */

   if((greaterRDD.count >= len)&&(lesserRDD.count >= len)) {
     val trim = greaterRDD.filter(word => word._2 < len).map(word => word._1).
            union(lesserRDD.filter(word => word._2 < len).map(word => word._1))

     val join = rdd.map(word => word._2 -> word._1)
            .join(trim.map(word => word -> 0))
            .map(word => word._2._1 -> word._1)
     join
   }
   /*
   if the no.of entries in the greaterRDD less than <len>  extract all entries from greaterRDD and
   <len> + (<len> - greaterRDD.count) no.of entries from lesserRDD
   */
   else if(greaterRDD.count < len) {

     val lenMod = len + (len - greaterRDD.count)
     val trim = greaterRDD.map(word => word._1)
            .union(lesserRDD.filter(word => word._2 < lenMod)
            .map(word => word._1))

     val join = rdd.map(word => word._2 -> word._1)
            .join(trim.map(word => word -> 0))
            .map(word => word._2._1 -> word._1)
     join
   }

   //if the no.of entries in the lesserRDD less than <len>
   //extract all entries from lesserRDD and
   //<len> + (<len> - lesserRDD.count) no.of entries from greaterRDD
   else {

     val lenMod = len + (len - lesserRDD.count)
     val trim = greaterRDD.filter(word => word._2 < lenMod).map(word => word._1).
            union(lesserRDD.map(word => word._1))

     val join = rdd.map(word => word._2 -> word._1).
            join(trim.map(word => word -> 0)).
            map(word => word._2._1 -> word._1)
     join
   }
  }

  /**
   * Computes the nearest neighbors in the data-set for the data-point against which KNN
   * has to be applied
   *
   * @param dataSet : RDD of Vectors of Int
   * @param dataPoint : Vector of Int
   * @param len : Number of data-points of the dataSet on which knnJoin is to be done
   * @param randomSize : the number of iterations which has to be carried out
   *
   * @return an RDD of Vectors of Int on which simple KNN needs to be applied with respect to the data-point
   */
  def knnJoin(dataSet : RDD[Vector[Int]],
      dataPoint : Vector[Int],
      len : Int,
      randomSize : Int,
      sc : SparkContext): RDD[Vector[Int]] = {

   val size = dataSet.first().length
   val rand = new Array[Int](size)
   val randomValue = new Random
   val rdd1 = dataSet.zipWithIndex()
   
   //compute z-value for each iteration, this being the first
   val model = zScore.computeScore(rdd1)
   val dataScore = zScore.scoreOfDataPoint(dataPoint)

   //for first iteration rand vector is a ZERO vector
   for(count <- 0 to size-1) rand(count) = 0

   //compute nearest neighbours on basis of z-scores
   val c_i = knnJoin_perIteration(rdd1, dataPoint, rand.toVector ,len,model, dataScore, sc)
   c_i.persist()

   //compute -> rdd where data-set generated from each iteration is being recursively appended
   var compute = c_i
   compute.persist()


   //the no.of iterations to be performed
   for(count <- 2 to randomSize) {

     for(i <- 0 to size - 1) rand(i) = randomValue.nextInt(100)


     //increment each element of the data-set with the random vector "rand"
     val newRDD = rdd1.map(vector => vector._1.zip(rand).map(word => word._1+word._2) -> vector._2)
     val newData_point = dataPoint.zip(rand).map(word => word._1 + word._2)

     //compute z-scores for the iteration
     val modelLooped = zScore.computeScore(newRDD)
     val data_scoreLooped = zScore.scoreOfDataPoint(newData_point)

     //compute nearest neighbours on basis of z-scores
     val c_iLooped = knnJoin_perIteration(newRDD, newData_point, rand.toVector, len, modelLooped, data_scoreLooped, sc)
     c_iLooped.persist()

     //remove the effect of random vector "rand" from each entry of the the returned RDD from knnJoin_perIteration

     val c_iCleansedLooped = c_iLooped.map(vector => vector._1.zip(rand).map(word => word._1 - word._2) -> vector._2)
     compute = compute.union(c_iCleansedLooped)
     compute.persist()
   }

    zKNN(removeRedundantEntries(compute), dataPoint, len)
  }

  /**
   * It removes redundant Vectors from the dataset
   * @param DataSet : RDD of Vector[Int] and the vectors corresponding line_no in the data-set
   * @return : RDD of non-repetitive Vectors on Int
   */
  def removeRedundantEntries(DataSet : RDD[(Vector[Int],Long)]) : RDD[Vector[Int]] = {
    DataSet.map(word => word._2 -> word._1).
          groupByKey().
          map(word => word._2.last)

  }

  /**
   * Computes euclidean distance between two vectors
   *
   * @param point1 : Vector of Int
   * @param point2 : Vector of Int
   * @return : euclidean distance between the two vectors
   */
  def euclideanDist(point1 : Vector[Int], point2 : Vector[Int]) : Double = {
    val sum = point1.zip(point2).map(word => pow(word._1 - word._2,2)).reduceLeft(_+_)
    sqrt(sum)
  }

  /**
   * Performs kNN over the modified data-set and returns the k-nearest neighbors for the data-point
   *
   * @param reducedData : RDD of Vector of Int, which is the reduced data-set after kNNJoin function applied to the data-set
   * @param dataPoint : Vector of Int, is the data-point for which kNN needs to be undertaken
   * @param k : the no.of neighbors to be computed
   * @return : RDD of Vector of Int
   */
  def zKNN(reducedData : RDD[Vector[Int]], dataPoint : Vector[Int], k : Int) : RDD[Vector[Int]] = {
    val distData = reducedData.map(word => euclideanDist(dataPoint, word) -> word)
            .sortByKey(true)
            .zipWithIndex()
            .filter(word => word._2 < k).map(word => word._1._2)
    distData

  }

}