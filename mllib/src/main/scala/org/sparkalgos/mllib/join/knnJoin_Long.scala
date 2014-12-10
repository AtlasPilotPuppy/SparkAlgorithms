/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sparkAlgos.mllib.join

import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import scala.util.Random

object knnJoin_Long {

  /**
   * Computes the nearest neighbors in the data-set for the data-point against which KNN
   * has to be applied for A SINGLE ITERATION
   *
   * @param rdd : RDD of Vectors of Long, which is the data-set in which knnJoin has 
   *		to be undertaken
   * @param dataPoint : Vector of Long, which is the data-point with which knnJoin is
   * 		done with the data-set
   * @param randPoint : Vector of Long, it's the random vector generated in each iteration
   * @param len : the number of data-points from the data-set on which knnJoin is to be done
   * @param zScore : RDD of (Long,Long), which is the ( <line_no> , <zscore> ) for each 
   * 		entry of the dataset
   * @param dataScore : Long value of z-score of the data-point
   * 
   * @return an RDD of the nearest 2*len entries from the data-point on which KNN needs to 
   * 		be undertaken for that iteration
   */
  def knnJoin_perIteration(rdd : RDD[(Vector[Long],Long)],
                           dataPoint : Vector[Long],
                           randPoint : Vector[Long],
                           len : Int,
                           zScore : RDD[(Long,BigInt)],
                           dataScore : BigInt,
                           sc : SparkContext) : RDD[(Vector[Long],Long)] = {


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
     val trim = greaterRDD.filter(word => word._2 < lenMod).map(word => word._1)
            .union(lesserRDD.map(word => word._1))

     val join = rdd.map(word => word._2 -> word._1)
            .join(trim.map(word => word -> 0))
            .map(word => word._2._1 -> word._1)
     join
   }
  }

  /**
   * Computes the nearest neighbors in the data-set for the data-point against which KNN
   * has to be applied
   *
   * @param dataSet : RDD of Vectors of Long
   * @param dataPoint : Vector of Long
   * @param len : Number of data-points of the dataSet on which knnJoin is to be done
   * @param randomSize : the number of iterations which has to be carried out
   *
   * @return an RDD of Vectors of Long on which simple KNN needs to be applied with 
   * 		respect to the data-point
   */
  def knnJoin(dataSet : RDD[Vector[Long]],
              dataPoint : Vector[Long],
              len : Int,
              randomSize : Int,
              sc : SparkContext): RDD[Vector[Long]] = {

   val size = dataSet.first().length
   val rand = new Array[Long](size)
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

     for(i <- 0 to size - 1) rand(i) = randomValue.nextInt(100).toLong


     //increment each element of the data-set with the random vector "rand"
     var kLooped = -1
     val newRDD = rdd1.map(vector => {kLooped = -1
                vector._1.map(word => word + rand({kLooped = kLooped+1
                kLooped%size})
                )} -> vector._2)


     val newData_point = dataPoint.map(word => word + rand({kLooped = kLooped+1
                kLooped % size}))


     //compute z-scores for the iteration
     val modelLooped = zScore.computeScore(newRDD)
     val data_scoreLooped = zScore.scoreOfDataPoint(newData_point)

     //compute nearest neighbours on basis of z-scores
     val c_iLooped = knnJoin_perIteration(newRDD, newData_point, rand.toVector, 
                                                  len, modelLooped, data_scoreLooped, sc)
     c_iLooped.persist()

     //remove the effect of random vector "rand" from each entry of the the returned RDD from 
     //knnJoin_perIteration
     var z_Looped = -1
     val c_iCleansedLooped = c_iLooped.map(line => {z_Looped = -1
                    line._1.map(word => word - rand({z_Looped = z_Looped+1
                    z_Looped%size})) } -> line._2)

     compute = compute.union(c_iCleansedLooped)
     compute.persist()
   }

    zKNN(removeRedundantEntries(compute), dataPoint, len).coalesce(1)
  }

  /**
   * It removes redundant Vectors from the dataset
   * @param DataSet : RDD of Vector[Long] and the vectors corresponding line_no in the data-set
   * @return : RDD of non-repetitive Vectors on Long
   */
  def removeRedundantEntries(DataSet : RDD[(Vector[Long],Long)]) : RDD[Vector[Long]] = {
    DataSet.map(word => word._2 -> word._1).
          groupByKey().
          map(word => word._2.last)

  }

  /**
   * Computes euclidean distance between two vectors
   *
   * @param point1 : Vector of Long
   * @param point2 : Vector of Long
   * @return : euclidean distance between the two vectors
   */
  def euclideanDist(point1 : Vector[Long], point2 : Vector[Long]) : Double = {
    var sum = 0.0
    for(i <- 0 to point1.length-1) {
      sum = sum + pow(point1(i) - point2(i),2)
    }
    sqrt(sum)
  }

  /**
   * Performs kNN over the modified data-set and returns the k-nearest neighbors for the data-point
   *
   * @param reducedData : RDD of Vector of Long, which is the reduced data-set after kNNJoin 
   * 		function applied to the data-set
   * @param dataPoint : Vector of Long, is the data-point for which kNN needs to be undertaken
   * @param k : the no.of neighbors to be computed
   * @return : RDD of Vector of Long
   */
  def zKNN(reducedData : RDD[Vector[Long]], dataPoint : Vector[Long], k : Int) : RDD[Vector[Long]] = {
    val distData = reducedData.map(word => euclideanDist(dataPoint, word) -> word)
            .sortByKey(true)
            .zipWithIndex()
            .filter(word => word._2 < k).map(word => word._1._2)
    distData

  }

}
