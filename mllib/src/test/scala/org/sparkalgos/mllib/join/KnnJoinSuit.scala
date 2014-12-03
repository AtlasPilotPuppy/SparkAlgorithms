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
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.sparkalgos.mllib.utils.LocalSparkContext

class KnnJoinSuit extends FunSuite with BeforeAndAfterEach with LocalSparkContext {

  var vectors: Vector[Vector[Int]] = _
  var data: RDD[Vector[Int]] = _
  var point:Vector[Int] = _
  var len: Int = _
  var iter :Int = _

  override def beforeEach() {

    /*
    data
    0, 0, 0
    1, 2, 3
    1, 5, 4
    5, 5, 8
    1, 1, 2
    1, 2, 4
    3, 4, 5
   */
    vectors = Vector(
      Vector(0, 0, 0),
      Vector(1, 2, 3),
      Vector(1, 5, 4),
      Vector(5, 5, 8),
      Vector(1, 1, 2),
      Vector(1, 2, 4),
      Vector(3, 4, 5)
    )
    data = sc.parallelize(vectors, 3)
    point = Vector(1,3,5)
    len = 4
    iter = 4
  }
    test("four neighbors should be there when length is four"){
      val model = KnnJoin.knnJoin(data,point,len,iter,sc)
      assert(model.count() == 4)

    }
    test("No neighbors should be computed when length is zero"){
      len = 0
      val model = KnnJoin.knnJoin(data,point,len,iter,sc)
      assert(model.count() == len)
    }

    test("All entries are from original data set") {
     val model = KnnJoin.knnJoin(data,point,len,iter,sc)
     assert(model.intersection(data).count() == len)

  }


/*
    test("knnJoin method called by the companion object") {
      val model = knnJoin.knnJoin(data,point,len,iter,sc)
      assert(model.getClass.getSimpleName.toString === "knnJoin")
    }
*/



  }
