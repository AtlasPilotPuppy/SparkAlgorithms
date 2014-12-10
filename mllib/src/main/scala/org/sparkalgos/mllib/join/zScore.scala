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
import java.util.logging.Logger

object zScore {

	/**
	 * Computes the z-score of a Vector
	 *  
	 * @param Vector of Long/Int
	 * @return z-score of the vector      
	 */
	val logger = Logger.getLogger("zScore")
	def scoreOfDataPoint[A](vector : Vector[A]) : BigInt = {
			val arg = vector(0)
					arg match{

					// if input is Vector[Int]
					case _: Int => 
					val vec = vector.map(word => word.toString.toInt)
					zScore_Int.scoreOfDataPoint(vec)

					// if input is Vector[Long]					
					case _: Long => 
					val vec = vector.map(word => word.toString.toLong)
					zScore_Long.scoreOfDataPoint(vec)


					case _ => logger.severe("Argument_0 to scoreOfDataPoint isn't of type Int/Long")
					exit(0)
			}
	}

	/**
	 * Computers the z-scores for each entry of the input RDD of Vector of Int/Long, sorted in ascending order
	 * 
	 * @param  rdd of Vector of Int/Long & Long
	 * @return z-scores of the RDD[( <line_no> , <z-value> )]
	 */
	def computeScore[A](rdd : RDD[(Vector[A],Long)])	: RDD[(Long,BigInt)] = {

			val arg = rdd.first._1
				arg(0) match{
			  
					// 	if input is Vector[Int]
					case _: Int => 
					val vec = rdd.map(line => line._1.map(f => f.toString.toInt) -> line._2)
					zScore_Int.computeScore(vec)
					
					// if input is Vector[Long]					
					case _: Long => 
					val vec = rdd.map(line => line._1.map(f => f.toString.toLong) -> line._2)
					zScore_Long.computeScore(vec)
					
					case _ => logger.severe("Argument_0 to scoreOfDataPoint isn't of type Int/Long")
					exit(0)
			}
	}
}
