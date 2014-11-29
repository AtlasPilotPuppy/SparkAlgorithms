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
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.google.common.hash._

/**
 * Get scores of the data-points by AVF algorithm 
 * score gives the data-point index and its score returned as a RDD
 * trimmed data is the data remained after user provided percentage of removal
 *
 */

class OutlierWithAVFModel private (
   val score:  RDD[(String,Int)],
   val trimedData:  RDD[Vector[String]],
   val outliers:  RDD[Vector[String]])

/**
 * Top-level methods for OutlierWithAVF.
 */

object OutlierWithAVFModel {
  /**
   * Computes the score of each data point which is summation of the frequency of 
   * each feature in that data-point. Low score data-points are outliers.
   *
   * @param input RDD of Vector[String] where feature values are comma separated .
   * @param hashSeed which is the hash-function to be used for representing data-points
   * @param sc is the Spark Context of the calling function
   * @return a RDD of hash-key and score.
   */
  def computeScores(input: RDD[Vector[String]],
                    hashSeed : HashFunction,
                    sc : SparkContext) : RDD[(String,Int)] =  {

    // key,value pairs for < (column_no,attribute value) , "frequency">
    val freq = input.map(word => word.zipWithIndex)
      .flatMap(line => line.toSeq)
      .map(word => word->1)
      .reduceByKey(_+_)
      .cache()

    // key,value pairs for < (column_no,attribute value) , "indexedInput-point number">
    val data = input.zipWithIndex().map(word => (word._2,word._1))
      .map(word => word._2.zipWithIndex
      .map(w =>  w-> hashSeed.hashLong(word._1).toString))
      .flatMap(line => line.toSeq)

    //join the two RDDs and get the frequency for each attribute in a indexedInput point
    val scores = data.join(freq)
      .flatMap(line => Seq(line.swap._1))
      .reduceByKey(_+_)
      .map(word => (word._1,word._2))

    scores
  }

  /**
   * On basis of the computed scores of data points and user-provided percentage of outliers 
   * to be removed, this functions removes the outliers from the input RDD and returns the 
   * trimmed data-set
   *
   * @param input RDD of Vector[String]
   * @param score of type (String, Int) having AVF score of the data-point obtained from 
   * function compute
   * @param percent of type Double which is the percentage of outliers to be removed from the 
   * data-set
   * @param hashSeed is the Hash-Function for uniquely identifying each data-point
   * @return trimmed data-set and outliers.
   */

  def trimScores(input : RDD[Vector[String]],
                 score : RDD[(String,Int)],
                 percent : Double,
                 hashSeed : HashFunction,
                 sc : SparkContext) : (RDD[Vector[String]],RDD[Vector[String]] )= {

    val nexample = score.count()
    val nremove =  nexample * percent*0.01

    //sorted scores
    val sortedScore = score.map(word => (word._2,word._1))
      .sortByKey(true)
      .map(word => (word._2,word._1))

    //trimmed score RDD
    val trimmedScores = sortedScore.zipWithIndex()
      .filter(word=> word._2 < nremove.toLong)
      .map(word => word._1).collect().toMap

    //filtered data-set
    val trimmedData = input.zipWithIndex()
      .map(word => (word._2,word._1))
      .map(word => hashSeed.hashLong(word._1).toString -> word._2)
      .filter(line => !trimmedScores.get(line._1).nonEmpty)
      .map(v => v._2)

    val outliers = input.zipWithIndex()
      .map(word => (word._2,word._1))
      .map(word => hashSeed.hashLong(word._1).toString -> word._2)
      .filter(line => trimmedScores.get(line._1).nonEmpty)
      .map(v => v._2)

    (trimmedData,outliers)

  }

  /**
   * This function acts as an entry point to compute the scores of the data-points and trim the
   * RDD's
   * @param data of type  RDD[Vector[String] ]
   * @param percent of type Double which is the percentage of outliers to be removed from the data-set
   * @param sc  Spark context
   * @return main.scala.OutlierWithAVFModel which has score RDD and trimmed data-set .
   */

  def outliers(data :RDD[Vector[String]],  percent : Double, sc :SparkContext) :OutlierWithAVFModel = {

    // initial check to validate user provided percentage
    if(percent >100){
      println("Error : percentage is greater than 100")
      System.exit(1)
    }

    // define a hash-function of type murmur3-128bits with seed_value of '5'
    val hashSeed = Hashing.murmur3_128(5)

    //compute the AVF scores for each data-point
    val scores = OutlierWithAVFModel.computeScores(data,hashSeed,sc)

    //returns an instance of main.scala.OutlierWithAVFModel
    val outlierData = OutlierWithAVFModel.trimScores(data, scores, percent,hashSeed,sc)
    val trimmed = outlierData._1
    val outliers = outlierData._2

    new OutlierWithAVFModel(scores,trimmed,outliers)

  }
}

