# Graph_processing
# Large Scale Supervised Link prediction with Spark Graph libraries and Spark MLlib

# Aims

1) To evaluate how the system performs link prediction on spark graph libraries and Spark MLLib
2) Comparison of frameworks.(Neo4j and GraphX).
3) Categorisation of features based on complexity and their contribution
4) Considerations on how to improve temporal aspects for working with graphs
5) Comparision of classifiers and Machine learning models for the link prediction between two authors.

# Project Structure

1) Project has been split up into many phases functionally and each functional requirement has self contained code(module) and dependencies. (Preprocess_input_text_to_parquet)
2) Each module has a main driver program which orchestrates the logic, a parameter constants file which includes application variables and a utilities file which actually consists of feature calculation logic , data loading and cleaning. The parameter file can be changed so that code can run both in local and on cluster.
3) We are trying to calculate 5 features such as pagerank, preferential attachment, common neighbours, first shortest path and second shortest path. We will log the run time in code and find out resources consumed(RAM/Disk/Network) on Yarn UI.
4) After features are calculated, they are assembled and machine learning algorithms are applied on all combinations of features. We will compare the better performing features and analyze the cost of calculating them. The features which cost more or adding no value can be removed.

# Project Flow

1) Preprocess RDF files to structured file. Our edges are directed and are directed from Paper to author. We don't have a direct undirected edge from author to author. Hence reduced the Paper to author edge to undirected edge between author and author so that its easy for computation. Store this in snappy compressed parquet.

2) Sample the complete graph using Connected components and write the components to a file.
- Pick required number of components that we could analyze on cluster. (generate_subgraphs_using_connectedcomponents_app)

3) Generate samples(author pairs) and training and testing samples(author pairs) and Graphs. To maintain class balance sample for both link exists and not exists almost equally.(generate_samples_and_subgraphs)

4) Calculate Pagerank (calculate_pagerank)

5) Calculate preferential attchment(calculate_preferential_attachment)

6) Calculate Common neighbours(calculate_common_neighbours)

7) Calculate First Shortest Path(calculate_first_shortest_path)

8) Calculate Second Shortest Path (calculate_second_shortest_path)

9) Assemble all the above graph topological features in single file(assemble_features)

10) Supervised Link prediction using different algorithms on all combinations of features

# Challenges Faced

1) Our Cluster was a comparitively smaller one and shared with 7 datanodes having maximun allocatable container per node was with 4 cores and 8 GB. This did not scale well for 2016/2017 and 2018 dataset which had 2.2 billion edges and 350 million vertices. But this scaled okay for 1989,1990 and 1991 dataset which had 10 million vertices and 80 million edges.

- Due to this we had to sample the 2016 graph and apply connected components on them. Later we choose only the number of components which we could work on in our cluster.

2) Connected components from Spark did not work for us, hence we need to use an algorithm called Big star / Small star algorithm (See Bottom for more details)

3) Path based algorithms from GraphX didn't scale well for us as edges increases. We couldn't apply Shortest paths or BFS for large number of samples on graph due to scaling issues. However this works fine for smaller samples.

Credits(Connected components Big star and small star algorithm) : Sirish Kumar: https://www.linkedin.com/pulse/connected-component-using-map-reduce-apache-spark-shirish-kumar/



