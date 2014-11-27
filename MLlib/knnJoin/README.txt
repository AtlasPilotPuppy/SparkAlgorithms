z-KNN
================================

##What's this? 
It's a modified knn-Join, which translates each multi-dimensional data-point into a single dimension on which KNN search for a data-point can be performed.
Given a data-set, the algorithm computes the z-values for each entry of the data-set and selects those entries with z-values closest to the z-value of the data-point. The process is performed over multiple iterations using random vector to transform the data-set. And then by using the data-entries over z-values, kNN is applied to the reduced data-set

##How to Run
You should have spark already build as a jar file in your build library path. It has a scala file with class 'knnJoin' and 'zScore'

From your main call the function "knnJoin" of this class, with following parameters
```
val model = knnJoin.knnJoin(dataset : RDD[Vector[Int]], datapoint : Vector[Int], len : Int, iteration : Int, sc : SparkContext)

model : RDD(Vector[Int])

It contains the kNN over the union of the all selected entried from the data-set as mentioned in 
http://ieeexplore.ieee.org/xpls/abs_all.jsp?arnumber=5447837&tag=1
```

