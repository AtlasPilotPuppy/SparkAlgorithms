/**
 * Created by ashu on 11/16/14.
 */

import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}

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
      val model = knnJoin.knnJoin(data,point,len,iter,sc)
      assert(model.count() == 4)

    }
    test("No neighbors should be computed when length is zero"){
      len = 0
      val model = knnJoin.knnJoin(data,point,len,iter,sc)
      assert(model.count() == len)
    }

    test("All entries are from original data set") {
     val model = knnJoin.knnJoin(data,point,len,iter,sc)
     assert(model.intersection(data).count() == len)

  }


/*
    test("knnJoin method called by the companion object") {
      val model = knnJoin.knnJoin(data,point,len,iter,sc)
      assert(model.getClass.getSimpleName.toString === "knnJoin")
    }
*/



  }
