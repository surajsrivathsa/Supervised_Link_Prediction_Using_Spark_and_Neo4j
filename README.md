# Large Scale Supervised Link prediction with Spark Graph libraries and Spark MLlib

# Contributors
1) Avinash Ranjan Singh
2) Khamar Zama
3) Madhu Sirivella
4) Pramod Bontha
5) Sanjeeth Busnur
6) Suraj Shashidhar

# Aims

1) To evaluate how the system performs link prediction on spark graph libraries and Spark MLLib
2) Comparison of frameworks.(Neo4j and GraphX).
3) Categorisation of features based on complexity and their contribution
4) Considerations on how to improve temporal aspects for working with graphs
5) Comparision of classifiers and Machine learning models for the link prediction between two authors.

# Project Structure

1) Project has been split up into many phases functionally and each functional requirement has self contained code(module) and dependencies. (Preprocess_input_text_to_parquet)
2) Each module has a main driver program which orchestrates the logic, a parameter constants file which includes application variables and a utilities file which actually consists of feature calculation logic , data loading and cleaning. The parameter file can be changed so that code can run both in local and on cluster.
3) We are trying initially to calculate 5 features such as pagerank, preferential attachment, common neighbours, first shortest path and second shortest path. Later we added Connected components itself as a feature as we hadto compute this due to below challenges mentioned. We will log the run time in code and find out resources consumed(RAM/Disk/Network) on Yarn UI.
4) After features are calculated, they are assembled and machine learning algorithms are applied on all combinations of features. We will compare the better performing features and analyze the cost of calculating them. The features which cost more or adding no value can be removed.

# Project Flow

1) Preprocess RDF files to structured file. Our edges are directed and are directed from Paper to author. We don't have a direct undirected edge from author to author. Hence reduce the Paper to author edge to undirected edge between author and author so that its easy for computation. Store this in snappy compressed parquet.

2) Sample the complete graph using Connected components and write the components to a file.
- Pick required number of components that we could analyze on cluster. (generate_subgraphs_using_connectedcomponents_app)

3) Generate samples(author pairs) and training and testing samples(author pairs) and Graphs. To maintain class balance sample for both link exists and not exists almost equally.(generate_samples_and_subgraphs)

4) Calculate Pagerank (calculate_pagerank)

5) Calculate preferential attchment(calculate_preferential_attachment)

6) Calculate Common neighbours(calculate_common_neighbours)

7) Calculate First Shortest Path(calculate_first_shortest_path)

8) Calculate Second Shortest Path (calculate_second_shortest_path)

9) Calculate Connected Component Feature (calculate_feature_connectedcomponent)

10) Assemble all the above graph topological features in single file(assemble_features)

11) Supervised Link prediction using different algorithms on all combinations of features (supervised_link_prediction)

# Challenges Faced

1) Our Cluster was a comparitively smaller one and shared with 7 datanodes having maximun allocatable container per node was with 4 cores and 8 GB. This did not scale well for 2016/2017 and 2018 dataset which had 2.2 billion edges and 350 million vertices. But this scaled okay for 1989,1990 and 1991 dataset which had 10 million vertices and 80 million edges.

- Due to this we had to sample the 2016 graph and apply connected components on them. Later we choose only the number of components which we could work on in our cluster. Hence we used connected components itself as one of the feature and surprisingly it performed relatively well for link prediction.

2) Connected components from Spark did not work for us, hence we need to use an algorithm called Big star / Small star algorithm (See Bottom for more details)

3) Path based algorithms from GraphX didn't scale well for us as edges increases. We couldn't apply Shortest paths or BFS for large number of samples on graph due to scaling issues. However this works fine for smaller samples.


Credits(Connected components Big star and small star algorithm) : Sirish Kumar: https://www.linkedin.com/pulse/connected-component-using-map-reduce-apache-spark-shirish-kumar/

# Results

1) Our Graph data was extremely skewed with very few huge connected components and large number of components with really less connections. This followed a power law distribution. 
2) We observed that Connected component performed much better with page rank coming in second and preferential attachment at third place, All these three features had average F1 scores around 0.7. Shortest path itself did not add much value, but boosted accuracy/F1 measure a bit when combined with Preferential attachment. But cost of calculating paths for huge graphs is more, hence it could be ignored in case of our type of scenario. Also Average F1 scores of top three performing features doesn't differ by much, Hence we could deduce for our dataset that we could skip computing connected components and just compute preferential attachment and pagerank which are much cheaper to calculate and achieve almost same results.
3) We observed that the link prediction performance goes up if we include more edges and vertices in our graph data and also if there is no class imbalance in samples.
4) Since we were able to extract four features for 1989/90 and 91 dataset, we got around 78% accuracy and good f1 score for preferential attachment and first shortest path combined. However preferential attachmnet itself was able to successfully classify around 75% of the data properly, Hence this was an MVF(most valuable feature) in our system.
5) For 2016/17 and 18 dataset, Since we sampled from graph our accuracy dropped to 66% for preferential attchment for graph with 12 Million edges and increased to 70% for graph with 30 Million edges. For First shortest path and preferential attachment from 12 Million graph combined we were getting accuracy around 63%. This was due to large number of false positives which occured due to sampling of the graph.Still Preferential attachment was the cheap and best feature that could classify the data for our dataset and setup.


# Project Report
Link to be updated

# Data Description

https://docs.microsoft.com/en-us/academic-services/graph/reference-data-schema



