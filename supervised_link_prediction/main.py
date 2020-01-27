# Author: Madhu Sirivella

import mlflow
import mlflow.spark
import itertools as it
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from argparse import ArgumentParser
from pyspark.ml.feature import VectorAssembler, StringIndexer
from classifiers import nbClassifier, dtClassifier, rfClassifier, gbtClassifier


spark = SparkSession.builder.appName("graphML").getOrCreate()
cumulatedResultList = list()
data_train_path = "D:\\2016,17,18_datasets\\training_features_20200124.csv"
data_test_path = "D:\\2016,17,18_datasets\\testing_features_20200124.csv"
columns = [['PR1', 'PR2'], ['PA1', 'PA2'], ['CN'],['FSP']]
fic = True #fic: feature importance check
seed = 5043
 #Schema definition
sch = StructType([ StructField("AId1", LongType(), False),
                  StructField("AId2", LongType(), False),
                  StructField("Year", IntegerType(), False),
                  StructField("Label", DoubleType(), False),
                  StructField("PR1", DoubleType(), False),
                  StructField("PR2", DoubleType(), False),
                  StructField("PA1", DoubleType(), False),
                  StructField("PA2", DoubleType(), False),
                  StructField("CN", DoubleType(), False),
                  StructField("FSP", DoubleType(), False)])

def generateDF(data_path, sch, columns):
    #Reading CSV
    data = spark.read.option("mode", "DROPMALFORMED").option("delimiter", "|").csv(data_path, schema = sch, header = "true")
    featuredf = data.select(columns)
    assembler = VectorAssembler(inputCols = columns[0:-1], outputCol="features")
    df = assembler.transform(featuredf).select(col("features"), col("Label").alias("label"))
    # df.describe().show()
    return df

def featureselection(columns):
    result = []
    for n in range(1, len(columns) + 1):
        temp = it.combinations(columns, n)
        for j in temp:
            result.append(list(it.chain.from_iterable(j)))
    return result


def main():
    nb = nbClassifier()
    dt = dtClassifier()
    rf = rfClassifier()
    gbt = gbtClassifier()

    if(fic):
        cols = featureselection(columns)

        for col in cols:
            col.append('Label')
            resultlist = list()
            dfTrain = generateDF(data_train_path, sch, col)
            dfTest = generateDF(data_test_path, sch, col)
            nbresult = nb.nbModel(dfTrain, dfTest, seed)
            print("columns used: {},Evaluation scores: {}".format(col, nbresult))
            resultlist.append("nb")
            resultlist.append(col)
            resultlist.append(nbresult)
            cumulatedResultList.append(resultlist)
            resultlist = list()
            dtresult = dt.dtModel(dfTrain, dfTest, seed)
            print("columns used: {},Evaluation scores: {}".format(col,dtresult))
            resultlist.append("dt")
            resultlist.append(col)
            resultlist.append(dtresult)
            cumulatedResultList.append(resultlist)
            rfresult = rf.rfModel(dfTrain, dfTest, seed)
            resultlist = list()
            resultlist.append("rf")
            resultlist.append(col)
            resultlist.append(rfresult)
            cumulatedResultList.append(resultlist)
            print("columns used: {},Evaluation scores: {}".format(col,rfresult))
            gbtresult = gbt.gbtModel(dfTrain, dfTest, seed)
            resultlist = list()
            resultlist.append("gbt")
            resultlist.append(col)
            resultlist.append(gbtresult)
            cumulatedResultList.append(resultlist)
            print("columns used: {},Evaluation scores: {}".format(col,gbtresult))
    else:
        dfTrain = generateDF(data_train_path, sch, columns)
        # dfTrain.show()
        dfTest = generateDF(data_test_path, sch, columns)
        # dfTest.show()
        nbresult = nb.nbModel(dfTrain, dfTest, seed)
        print(nbresult)
        dtresult = dt.dtModel(dfTrain, dfTest, seed)
        print(dtresult)
        rfresult = rf.rfModel(dfTrain, dfTest, seed)
        print(rfresult)
        gbtresult = gbt.gbtModel(dfTrain, dfTest, seed)
        print(gbtresult)

    print('Final results')
    print(cumulatedResultList)


if __name__ == "__main__":
    main()