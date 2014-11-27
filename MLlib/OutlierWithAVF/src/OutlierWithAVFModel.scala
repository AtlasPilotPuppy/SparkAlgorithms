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
import org.apache.spark.mllib.util.MLUtils
import scala.math._
import com.google.common.hash._

/**
 * Get scores of the data-points by AVF algorithm 
 * score gives the data-point index and its score returned as a RDD
 * trimmed data is the data remained after user provided percentage of removal
 * 
 */
class OutlierWithAVFModel private (
    val score:  RDD[(String,Int)],
    val trimmed_data:  RDD[Vector[String]] )

/**
 * Top-level methods for OutlierWithAVF.
 */
 object OutlierWithAVFModel {
  /**
   * Computes the score of each data point which is summation of the frequency of 
   * each feature in that data-point. Low score data-points are outliers.
   * 
   * @param input RDD of Vector[String] where feature values are comma separated .
   * @param hash_seed which is the hash-function to be used for representing data-points
   * @param sc is the Spark Context of the calling function
   * @return a RDD of hash-key and score.
   */
  def compute(input: RDD[Vector[String]], 
      hash_seed : HashFunction, 
      sc : SparkContext) : RDD[(String,Int)] =  {
       
    //obtain the no.of features of each data-point
    val counter = input.first().length
    val x = new Array[Int](counter)
    for(i <- 0 to counter) x(i) = i
    
    // key,value pairs for < (column_no,attribute value) , "frequency">
    val rdd1 = input.map(word => word.zip(x))
      .flatMap(line => line.toSeq)
      .map(word =>(word)->1)
      .reduceByKey(_+_)
      .cache()
   
    // key,value pairs for < (column_no,attribute value) , "data-point number">
    val len = sc.parallelize(1L to input.count)
    val data = len.zip(input)
    
    val rdd2 = data.map(word => word._2.zip(x)
                .map(w =>  w->hash_seed.hashLong(word._1).toString()))
                .flatMap(line => line.toSeq)  
    
     //join the two RDDs and get the frequency for each attribute in a data point
    val rdd_joined = rdd2.join(rdd1)
      .flatMap(line => Seq(line.swap._1))
      .reduceByKey(_+_)
      .map(word => ((word._1),word._2))
    
      rdd_joined//.saveAsTextFile("/home/ashu/Desktop/joined")
  }
 
  /**
   * On basis of the computed scores of data points and user-provided percentage of outliers 
   * to be removed, this functions removes the outliers from the input RDD and returns the 
   * trimmed data-set
   * 
   * @param input RDD of Vector[String]
   * @param score_RDD of type (String, Int) having AVF score of the data-point obtained from 
   * function compute
   * @param percent of type Double which is the percentage of outliers to be removed from the 
   * data-set
   * @param hash_seed is the Hash-Function for uniquely identifying each data-point
   * 
   * @return OutlierWithAVFModel which has score RDD and trimmed data-set .
   */
  
  def trimScores(
      input : RDD[Vector[String]], 
      score_RDD : RDD[(String,Int)], 
      percent : Double, 
      hash_seed : HashFunction,
      sc : SparkContext) : OutlierWithAVFModel = {
    
    val nexample = score_RDD.count()
    val nremove =  (nexample * percent*0.01)
    
    //sorted scores
    val sort_score = score_RDD.map(word => (word._2,word._1))
      .sortByKey(true)
      .map(word => (word._2,word._1))
     
     //trimmed score RDD 
     val data_trim = sort_score.zipWithIndex
       .filter(word=> word._2 < nremove.toLong)
       .map(word => word._1).collect.toMap
    
    //filtered dataset  
    val alter_data = sc.parallelize(1L to input.count)
      .zip(input) 
      .map(word => hash_seed.hashLong(word._1).toString() -> word._2)
      .filter(line => !(data_trim.get(line._1).nonEmpty))
      .map(v => v._2) 
    
   
    new OutlierWithAVFModel(score_RDD, alter_data)

  }
  
  /**
   * This function acts as an entry point to compute the scores of the data-points and trim the
   * RDD's
   * @param data of type  RDD[Vector[String]] 
   * @param percent of type Double which is the percentage of outliers to be removed from the data-set
   * @param Sparkcontext
   * @return OutlierWithAVFModel which has score RDD and trimmed data-set .
   */
  
  def outliers(data :RDD[Vector[String]],  percent : Double, sc :SparkContext) :OutlierWithAVFModel = {
   
   // initial check to validate user provided percentage 
     if(percent >100){
      println("Error : percentage is greater than 100")
     System.exit(1)
    }
     
    // define a hash-function of type murmur3-128bits with seed_value of '5'
    val hash_seed = Hashing.murmur3_128(5) 
    
    
    //compute the AVF scores for each data-point
    val score_RDD = OutlierWithAVFModel.compute(data,hash_seed,sc)
    
    //returns an instance of OutlierWithAVFModel
    val model = OutlierWithAVFModel.trimScores(data, score_RDD, percent,hash_seed,sc) 
  
    model
     
  }
 }

