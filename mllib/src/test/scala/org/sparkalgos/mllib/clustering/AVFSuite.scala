  /**
   * Created by ashu on 11/16/14.
   */

  import org.apache.spark.rdd.RDD

  import org.scalatest.{BeforeAndAfterEach, FunSuite}

  class outSuit extends FunSuite with BeforeAndAfterEach with LocalSparkContext {

    var vectors: Vector[Vector[String]] = _
    var data: RDD[Vector[String]] = _

    override def beforeEach() {

      /*
      data score
      A,B   5
      A,C   4
      A,D   4
      E,B   3
     */
      vectors = Vector(
        Vector("A", "B"),
        Vector("A", "C"),
        Vector("A", "D"),
        Vector("E", "B")
      )
      data = sc.parallelize(vectors, 2)
    }
      test("only two outliers should be removed"){
        val model = OutlierWithAVFModel.outliers(data,30,sc)
        assert(model.trimedData.count() == 3)

      }
      test("No outlier should be removed"){
        val model = OutlierWithAVFModel.outliers(data,0,sc)
        assert(model.trimedData.count() == 4)
      }

      test("4 entries in score RDD") {
       val model = OutlierWithAVFModel.outliers(data, 30, sc)
       assert(model.score.count() == 4)

    }

    test("with 30 percent outliers 1 entry outlier RDD") {
      val model = OutlierWithAVFModel.outliers(data, 30, sc)
      assert(model.outliers.count() === 1)

    }

    test("vector(E,B) should be outlier"){
      val model = OutlierWithAVFModel.outliers(data, 30, sc)
      assert(model.outliers.first().equals(Vector("E", "B")))

    }

    test("outlires method called by the companion object") {
        val model = OutlierWithAVFModel.outliers(data, 30, sc)
        assert(model.getClass.getSimpleName.toString === "OutlierWithAVFModel")
      }




    }
