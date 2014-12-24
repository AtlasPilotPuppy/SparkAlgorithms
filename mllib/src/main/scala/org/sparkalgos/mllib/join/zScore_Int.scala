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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Vector
import scala.math.BigInt

object zScore_Int {
  
	/**
	 * Checks if all entries within the array are 0 or not
	 * 
	 * @param Array of Int
	 * @return 1, if all elements are zero; else 0
	 */
	def checkVectors(vector : Array[Int]) : Int = {
			var flag = 1

					for(i <- 0 to vector.length - 1){
						if(vector(i)!=0){
							flag = 0
						}
					}

			return flag
	}
  
  /**
   * Computers the z-scores for each entry of the input RDD of Vector of Int, sorted 
   * in ascending order
   * 
   * @param  rdd of Vector of Int
   * @return z-scores of the RDD[( <line_no> , <z-value> )]
   */
  def computeScore(rdd : RDD[(Vector[Int],Long)])	: RDD[(Long,BigInt)] = {

    val score = rdd.map(word => scoreOfDataPoint(word._1) -> word._2).
    			sortByKey(true).
    			map(word => word._2 -> word._1)
    
    score
        
  }
  
   /**
   * Computes the z-score of a Vector
   *  
   * @param Vector of Int
   * @return z-score of the vector      
   */
  def scoreOfDataPoint(vector : Vector[Int]) : BigInt = {
 
    var x = vector.toArray
    
    var temp = 0
    var score : BigInt = 0
    var counter = 0
    
    while(checkVectors(x) == 0) {
      for(i <- x.length-1 to 0 by -1){
        temp = x(i) & ((1 << 1) - 1)
        temp = temp << counter
        score = score+temp
        x(i) = x(i)>>1
        counter = counter + 1
      }
    }
    score
  }
  
}
