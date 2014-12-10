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
import java.util.logging.Logger

object KnnJoin {

  /**
	 * Computes the nearest neighbors in the data-set for the data-point against which KNN
	 * has to be applied
	 *
	 * @param dataSet : RDD of Vectors of Int/Long
	 * @param dataPoint : Vector of Int/Long
	 * @param len : Number of data-points of the dataSet on which knnJoin is to be done
	 * @param randomSize : the number of iterations which has to be carried out
	 *
	 * @return an RDD of Vectors of Int/Long on which simple KNN needs to be applied with respect to
	 * 		the data-point
	 */
	def knnJoin[A](dataSet : RDD[Vector[A]],
			dataPoint : Vector[A],
			len : Int,
			randomSize : Int) = {

		val logger = Logger.getLogger("knnJoin")
		val sc = dataSet.context

				val arg = dataPoint(0)
				arg match{

			// if input is RDD[Vector[Int]]
				case _: Int => 
				println("Calling Int")
				val set = dataSet.map(f => f.map(word => word.toString.toInt))
				val point = dataPoint.map(f => f.toString.toInt)
				knnJoin_Int.knnJoin(set, point, len, randomSize, sc).coalesce(1)

				// if input is RDD[Vector[Long]]	      
				case _: Long =>
				println("Calling Long")
				val set = dataSet.map(f => f.map(word => word.toString.toLong))
				val point = dataPoint.map(f => f.toString.toLong)
				knnJoin_Long.knnJoin(set, point, len, randomSize, sc).coalesce(1)

				case _ => logger.severe("Argument_0 to knnJoin isn't of type Int/Long")
				exit(0)

		}
	}


}
