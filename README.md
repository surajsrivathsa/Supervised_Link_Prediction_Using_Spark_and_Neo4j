# Graph_processing
# Large Scale Supervised Link prediction with Spark Graph libraries and Spark MLlib

# Aims

1) To evaluate how the system performs link prediction on spark graph libraries and Spark MLLib
2) Comparison of frameworks.(Neo4j and GraphX).
3) Categorisation of features based on complexity and their contribution
4) Considerations on how to improve temporal aspects for working with graphs
5) Comparision of classifiers and Machine learning models for the link prediction between two authors.

# Project Structure

1) Project has been split up into many phases functionally and each functional requirement has self contained code(module) and dependencies.
2) Each module has a main driver program which orchestrates the logic, a parameter constants file which includes application variables and a utilities file which actually consists of feature calculation logic , data loading and cleaning. The parameter file can be changed so that code can run both in local and on cluster.
3) We are trying to calculate 5 features such as pagerank, preferential attachment, common neighbours, first shortest path and second shortest path. We will log the run time in code and find out resources consumed(RAM/Disk/Network) on Yarn UI.
4) After features are calculated, they are assembled and machine learning algorithms are applied on all combinations of features. We will compare the better performing features and analyze the cost of calculating them. The features which cost more or adding no value can be removed.

# Challenges Faced

1) Our Cluster was a comparitively smaller one and shared with 7 datanodes having maximun allocatable processors as 4 and executor memory as 8 GB. This did not scale well for 2016/2017 and 2018 dataset which had 2.2 billion edges and 350 million vertices. But this scaled okay for 1989,1990 and 1991 dataset which had 10 million vertices and 80 million edges.

- Due to this we had to sample the 2016 graph and apply connected components on them. Later we choose only the number of components which we could work on in our cluster.

2) Path based algorithms from GraphX doesn't scale well as edges increases. We couldn't apply Shortest paths or BFS for large number of samples on graph due to scaling issues. However this works fine for smaller samples.



