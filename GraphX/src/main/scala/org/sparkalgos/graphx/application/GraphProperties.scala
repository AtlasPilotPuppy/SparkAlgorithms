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

package org.sparkalgos.graphx.application

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection._
import scala.collection.mutable.Seq
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


/**
 * Get the vertices of a directed Graph that can reach to a given point
 * This algorithm can find application in Geographic Information Systems
 * to compute the points in the map from where water can flow to a given co-ordinate
 */
class GraphProperties (
                        val RDDvertex:  RDD[(VertexId, (Iterable[Long], Int, Int))],
                        val RDDedge:  RDD[Edge[Int]],
                        val RDDcoOrdinate_vertexID_Mapper : RDD[((Long,Long),Long)],
                        val srcID : Long
                        )


/**
 *     top level methods for watershed algorithm using pregel API
 */
object WaterShed {

  /**
   * Converts a DigitalElevationMap of the form x,y,z to
   * a RDD of Vertex , a RDD of Edge from which graphs can be created,
   * each co-ordinate now being represented in the form of a vertex
   * and the direction of flow of water from one co-ordinate to another being
   * represented in terms of an edge between the vertices (i.e. co-ordinates)
   *
   * @param path : location of the DigitalElevationMap(DEM)
   * @param sc : spark context
   * @param x_Point : x-Coordinate of point for which delineation needs to be done
   * @param y_Point : y-Coordinate of point for which delineation needs to be done
   * @return an object of GraphProperties comprising of
   * 		RDD of Vertices and it's properties
   *   		RDD of Edges and it's properties
   *     	RDD of mappings between co-ordinates and vertexId's
   *      	vertexID of the point for which delineation is to be performed
   */
  private def conversion_DEMcsv_to_RDDed_RDDvd(path : String, sc : SparkContext, x_Point : Long, y_Point : Long) : GraphProperties = {

    //read csv file of the form < x,y,z >
    val temp_data = sc	.textFile(path)
      .map(word => 	((word	.split(","))
      .map(lit => lit.toDouble)
      .toList
      ).toVector)
      .map(word => word(0) -> (word(1) -> word(2)))

    val x_len = temp_data.groupBy(f => f._1).count
    val y_len = temp_data.groupBy(f => f._2._1).count

    //alter co-ordinate values, in case x and y values do not start from 0
    val data = temp_data.groupByKey.sortByKey(true)
      .zipWithIndex
      .map(word => word._1._2.map(w => w._1 -> (word._2 -> w._2)) ).flatMap(f => f)
      .groupByKey.sortByKey(true)
      .zipWithIndex
      .map(word => word._1._2.map(w => (w._1 -> word._2) -> w._2)).flatMap(f => f)
      .zipWithIndex
      .map(word => word._1._1 -> (word._1._2 -> word._2))
    val mapper = data.map(word => word._1 -> word._2._2)

    //sub-routine to compute edges for each vertex
    val mod_data1 = data.map(word => ((word._1._1 + 1),word._1._2) -> word)
    val mod_data2 = data.map(word => ((word._1._1 - 1),word._1._2) -> word)
    val mod_data3 = data.map(word => (word._1._1,(word._1._2 - 1)) -> word)
    val mod_data4 = data.map(word => (word._1._1,(word._1._2 + 1)) -> word)
    val mod_data5 = data.map(word => ((word._1._1 + 1),(word._1._2 - 1)) -> word)
    val mod_data6 = data.map(word => ((word._1._1 + 1),(word._1._2 + 1)) -> word)
    val mod_data7 = data.map(word => ((word._1._1 - 1),(word._1._2 - 1)) -> word)
    val mod_data8 = data.map(word => ((word._1._1 - 1),(word._1._2 + 1)) -> word)


    val mod_data = mod_data1.union(mod_data2).union(mod_data3).union(mod_data4)
      .union(mod_data5).union(mod_data6).union(mod_data7)
      .union(mod_data8).filter(word => (word._1._1 >= 0) && (word._1._1 < x_len))
      .filter(word => (word._1._2 >= 0) && (word._1._2 < y_len))
      .groupByKey
      .sortByKey(true)
      .map(word => word._1 -> {

      /**
       * (NEW)
       * let it return all the nearest set of points
       */
      word._2
    })
    // creates an edge from vertex1 to all other vertices which have elevation less than or equal to vertex1
    val join = data.join(mod_data).map(w => w._1 -> (w._2._1 -> w._2._2.filter(f => f._2._1 <= w._2._1._1)))
    val edgeRDD : RDD[Edge[Int]] = join	.map(word => word._2._1._2 -> word._2._2.map(f => f._2._2))
      .map(f => f._2.map(d => f._1 -> d))
      .flatMap(f => f)
      .map(f => Edge(f._1 , f._2,1)).coalesce(1)

    //sub-routine to compute vertices
    val vertexRDD : RDD[(VertexId, (Iterable[Long], Int, Int))]
    = join	.map(word => word._2._1._2 -> (word._2._2.map(f => f._2._2) ,0,0))
      .coalesce(1)

    val srcID = mapper.filter(f => f._1._1 == x_Point && f._1._2 == y_Point).first._2

    new GraphProperties(vertexRDD,edgeRDD,mapper,srcID)
  }

  /**
   * Computes all vertices in a directed graph which can reach to a given vertex
   *
   * @param path : location of the DigitalElevationMap(DEM)
   * @param sc : spark context
   * @param x_Point : x-Coordinate of point for which delineation needs to be done
   * @param y_Point : y-Coordinate of point for which delineation needs to be done
   * @return RDD of co-ordinates which can reach to the given vertex
   */

  def connectedComponents_to_point(sc : SparkContext, path : String, x_Point : Long, y_Point : Long  ) : RDD[(Long,Long)] = {


    val model = conversion_DEMcsv_to_RDDed_RDDvd(path, sc, x_Point , y_Point)

    //create graph from the returned RDD of vertices and edges
    val graph = Graph(model.RDDvertex, model.RDDedge)
    val sourceId: VertexId = model.srcID

    // Initialize the graph to identify the source vertex from which delineation needs to be performed
    val initialGraph = graph.mapVertices((id, word) => if (id == sourceId) (word._1,1 , 0) else word)

    //sub-routine for delineation computation
    val cctp = initialGraph.pregel(Seq(0L))(

      /**
       * Vertex Program
       */
      (id, prop, newID) =>
      { println("VERTEXVALUE : " + id +","+prop+","+newID)
        if(prop._3 == 0) {
          (prop._1,prop._2,1)
        }
        else {
          if(prop._2 == 1) {
            (prop._1,2,prop._3)
          }
          else if(prop._2 == 0){
            if(prop._1.toSeq.map(f => if(newID.isEmpty) 0 else {if(newID.contains(f)) 1 else 0})	.filter(d => d==1).length > 0)
            {
              (prop._1,1,prop._3)
            }
            else prop
          }
          else prop

        }
      },
      /**
       * Message computation for each triplet
       */
      triplet => {

        if (triplet.dstAttr._2 == 1){
          Iterator((triplet.srcId,Seq(triplet.dstId))) ++ Iterator((triplet.dstId,Seq(triplet.dstId)))//,(triplet.dstId,Seq(triplet.dstId)))
        }
        else if (triplet.dstAttr._2 == 2 && triplet.srcAttr._2 == 1) {
          Iterator((triplet.srcId,Seq(triplet.dstId)))
        }
        else {
          Iterator.empty
        }


      },
      (a,b) => a++b //Merge message
    )

    //vertices from the new graph which reach out to the vertex
    cctp.vertices	.map(f => f._1.toLong -> f._2._2)
      .join(model.RDDcoOrdinate_vertexID_Mapper.map(w => w._2 -> w._1))
      .map(w => w._2._2 -> w._2._1).filter(word => word._2 == 2).map(f => f._1)


  }
}