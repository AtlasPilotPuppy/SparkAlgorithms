Watershed Delineation using GraphX
================================

##What's this? 
This algorithm computes all vertices in a directed graph, that can reach out to a given vertex. It finds use in varied applications, one being Geographic Information Systems to compute the watershed delineation of a co-ordinate i.e. the various co-ordinates in the map from where water can flow to a given co-ordinate

##Contents : 
```
test.scala - provides info. on how to use this function
pregel_watershed.scala - the actual file with the coded algorithm
data.csv - sample data which needs to be input
output - sample output for datapoint with co-ordinate(2,2)
```

![Alt text](http://www.markhneedham.com/blog/wp-content/uploads/2013/07/betweeness2.png "Optional title")

The vertices that can reach out to Vertex 'E' are : Vertices 'A' and 'B'
