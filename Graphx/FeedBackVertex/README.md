#Finding Feedback Vertex Set 

## A greedy solution

A greedy recursive solution to find feedback vertex set of a directed graph.


Feedback vertex set is a set of vertices in a directed graph removing which, will eliminate 
all the cycles in the graph. Our algorithm takes the idea of disjoint strongly connected components
and work on them in parallel. Since breaking a strongly connected component(scc) breakes all the simple 
cycles it has.


We find the optimal vertex which break the scc into maximum strongly connected components
