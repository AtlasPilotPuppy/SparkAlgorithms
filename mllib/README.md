##MLlib

This folder contains the implementation of  additional machine learning algorithms which can be used 
with apapche Spark
Outlier-Detection-with-AVF-Spark
================================

##What's this? 
This is an outlier detection algorithm which works on categorical data. It calculated the frequency of occurence of each attribute of a data-point within the entire dataset. Based on these frequencies scores are assigned to each data point Data points with minimum scores are the designated outliers.

##How to Run
You should have spark already build as a jar file in your build library path. It has a scala file with class 'OutlierWithAVFModel'


From your main call the function "outliers" of this class, with following parameters
```
 val sc = new SparkContext("local", "OutlierDetection")
  val dir = "hdfs://localhost:54310/train3"      <your file path>
   
   val data = sc.textFile(dir).map(word => word.split(",").toVector)
   val model = OutlierWithAVFModel.outliers(data,20,sc) 
   
   model.score.saveAsTextFile("../scores")
   model.trimmed_data.saveAsTextFile(".../trimmed")
   
returned model has two attributes  score and trimmed_data.

model.score :       RDD(String, Int)
It contains the hash key representation of a datapoint and its avf score.

model.trimmed_data: RDD(String)
It contains the dataset minus the outliers by the percentage provided.
```

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

