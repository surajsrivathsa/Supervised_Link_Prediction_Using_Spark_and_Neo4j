import csv
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
data_train_path = "D:\\2016,17,18_datasets\\assembled_features_training_all_20200129.csv"
data_test_path = "D:\\2016,17,18_datasets\\assembled_features_testing_all_20200129.csv"
output_file_path = "D:\\2016,17,18_datasets\\30MDatasetresults.csv"
columns = [['DPR1', 'DPR2'], ['PA1', 'PA2'], ['CN'], ['CC']]

#fic: feature importance check
fic = True

#Schema definition
sch = StructType([ StructField("AId1", LongType(), False),
                  StructField("AId2", LongType(), False),
                  StructField("Year", IntegerType(), False),
                  StructField("Label", DoubleType(), False),
                  StructField("DPR1", DoubleType(), False),
                  StructField("DPR2", DoubleType(), False),
                  StructField("PR1", DoubleType(), False),
                  StructField("PR2", DoubleType(), False),
                  StructField("PA1", DoubleType(), False),
                  StructField("PA2", DoubleType(), False),
                  StructField("CN", DoubleType(), False),
                  StructField("FSP", DoubleType(), False),
                  StructField("CC", DoubleType(), False)])

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

def write_csv(result):
    with open(output_file_path, "w+") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=",")
        for row in result:
            csv_writer.writerow(row)

def main():
    nb = nbClassifier()
    dt = dtClassifier()
    rf = rfClassifier()
    gbt = gbtClassifier()

    if(fic):
        cols = featureselection(columns)

        for col in cols:
            col.append('Label')
            dfTrain = generateDF(data_train_path, sch, col)
            dfTest = generateDF(data_test_path, sch, col)
            nbresult = nb.nbModel(dfTrain, dfTest, col)
            print(nbresult)
            cumulatedResultList.append(nbresult)
            dtresult = dt.dtModel(dfTrain, dfTest, col)
            print(dtresult)
            cumulatedResultList.append(dtresult)
            rfresult = rf.rfModel(dfTrain, dfTest, col)
            print(rfresult)
            cumulatedResultList.append(rfresult)
            gbtresult = gbt.gbtModel(dfTrain, dfTest, col)
            print(gbtresult)
            cumulatedResultList.append(gbtresult)
    else:
        dfTrain = generateDF(data_train_path, sch, columns)
        # dfTrain.show()
        dfTest = generateDF(data_test_path, sch, columns)
        # dfTest.show()
        nbresult = nb.nbModel(dfTrain, dfTest, columns)
        print(nbresult)
        dtresult = dt.dtModel(dfTrain, dfTest, columns)
        print(dtresult)
        rfresult = rf.rfModel(dfTrain, dfTest, columns)
        print(rfresult)
        gbtresult = gbt.gbtModel(dfTrain, dfTest, columns)
        print(gbtresult)
    print('Final results')
    print(cumulatedResultList)
    write_csv(cumulatedResultList)


if __name__ == "__main__":
    main()