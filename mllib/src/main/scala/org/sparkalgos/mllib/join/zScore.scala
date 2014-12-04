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

object zScore {
  
  /**
   * Formats the integer to it's binary value with 0's spaced to the left of the binary string
   * i.e. * asNdigitBinary(3,4) = 0011
   * 
   * @param source : Integer to be formatted to it's binary value
   * @param digits : the length of the binary string
   * @return binary value of <source> with length of the string equal to <digits>
   */

  def asNdigitBinary (source: Int, digits: Int): String = {
		val l: java.lang.Long = source.toBinaryString.toLong
		String.format ("%0" + digits + "d", l) 
  }
  

  /**
   * Computes the z-value of each Vector[Int]
   * i.e. Vector(2,6) z-value will be 28
   * @param vector : a Vector of Integers which refers to a data-point
   * @return z-value of the vector 
   */
  def scoreOfDataPoint(vector : Vector[Int]) : Long = {
 
    var max = 0

    //compute the length of the largest binary string in the vector of integers
    for(i <- 0 to vector.length-1){
      if (vector(i).toBinaryString.length() > max ) max = vector(i).toBinaryString.length()
    }

    var str = new StringBuilder(max * vector.length )

    //map each integer within the vector to a formatted binary string of length <max>
    val bin2 = vector.map(word => asNdigitBinary(word, max))

    //create the string which is the binary string(z-value) for the input vector
    for(i <- 0 to max-1) {
      for(j <- 0 to vector.length-1) {
        
        str += bin2(j)(i)
      }
    }
  
    //convert the binary string(z-value) to it's corresponding Integer value
    Integer.parseInt(str.toString(), 2).toLong
  }
  
  /**
   * Computers the z-scores for each entry of the input RDD of Vector of Int, sorted in ascending order
   * 
   * @param  rdd[(Vector[Int],Long)]) of Vector of Int
   * @return z-scores of the RDD[( <line_no> , <z-value> )]
   */
  def computeScore(rdd : RDD[(Vector[Int],Long)])	: RDD[(Long,Long)] = {

    val score = rdd.map(word => scoreOfDataPoint(word._1) -> word._2).
    			sortByKey(true).
    			map(word => word._2 -> word._1)
    
    score
        
  }
  
}
