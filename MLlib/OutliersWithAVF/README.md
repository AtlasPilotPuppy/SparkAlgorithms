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

