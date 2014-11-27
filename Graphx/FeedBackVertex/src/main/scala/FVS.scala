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
import org.apache.spark.graphx._


object FVS {

  def main(args: Array[String]) {

    val sc = new SparkContext("local","FVS")
    var res = sc.parallelize(Array[Long]())

    //build graph
    val edges =
      Array(1L -> 4L,  1L -> 2L,   2L -> 5L,   2L -> 3L,  3L ->5L,    3L->9L) ++
        Array(4L -> 5L,  4L -> 6L,   6L -> 5L,   6L -> 7L,  7L -> 6L,   7L -> 3L,  7L -> 9L) ++
        Array(8L -> 10L, 8L -> 6L,   9L -> 10L,  9L -> 1L,  10L -> 7L,  10L -> 8L)

    val rawEdges = sc.parallelize(edges)
    val graph = Graph.fromEdgeTuples(rawEdges, -1).mapVertices((id,attr) => id.toLong)

    res = res.union(FVSModel.feedbackVertexSet(graph,sc)).coalesce(1)
    res.persist()
    res.saveAsTextFile("/home/ashu/Desktop/k")
  }
}