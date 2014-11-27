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
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
/*
 * Top level model for feedback vertex set
 */
object FVSModel {

  /**
   * This function calculate the optimal vertex in one scc, by removal of which highest number of scc
   * is generated.
   * @param graph of type  Graph[Long,Int] 
   * @param sc SparkContext
   * @return rdd with the optimal set of vertices. 
   */
  def getVertex(graph: Graph[Long, Int],
                sc:SparkContext): RDD[Long] = {

    val vertices = graph.vertices.collect()

    //work only for the vertex size more then 1
    if(vertices.size > 1) {

      val z = vertices.map(id => id._1 ->
        graph.subgraph(vpred = (index, scc) => index != id._1)
          .stronglyConnectedComponents(2).vertices.map(word => word._2->word._1)
          .groupBy( word => word._1)
          .count)
      //get the vertex with max scc
      val vMax = z.reduce( (a,b) => if (a._2 > b._2) a else b )
      val idMax = vMax._1

      var vList = sc.parallelize(Array(vMax._1))
      vList.persist()
      //remove the max id vertex and run the algorithm again
      vList.union(feedbackVertexSet( graph.subgraph(vpred = (index, scc) => index != idMax) ,sc))
        .coalesce(1)
      vList.persist()

      vList
    }
    else
      sc.parallelize(Array[Long]())
  }

  /**
   * This function calculate the strongly connected components and run getVertex on each scc in parallel
   * @param graph of type  Graph[Long,Int] 
   * @param sc Sparkcontext
   * @return rdd with the optimal set of vertices. 
   */
  def feedbackVertexSet(graph:Graph[Long,Int],
                        sc :SparkContext ): RDD[Long] = {

    //calculate strongly connected components
    val sccGraph = graph.stronglyConnectedComponents(2)

    var res = sc.parallelize(Array[Long]())
    //get the component ids (minimum id in each component) in the list l
    val l = sccGraph.triplets.map(word => Array(word.srcAttr, word.dstAttr)).flatMap(f =>f)
      .map(w => w->0).groupBy(f => f._1).map(f => f._1).collect()

    //run get vertices on each component
    l.map(id => {res = res
      .union(getVertex( sccGraph.subgraph(vpred = (index, scc) => scc == id ),sc))
      .coalesce(1)
      res.persist()
    } )
    res
  }
}






