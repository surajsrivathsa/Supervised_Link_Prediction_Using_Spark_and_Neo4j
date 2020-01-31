import mlflow
import mlflow.spark
from pyspark.ml.classification import NaiveBayes, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.sql.functions import *


class nbClassifier:
    def nbModel(self, dfTrain, dfTest, seed):
        client = mlflow.tracking.MlflowClient()
        mlflow.set_experiment("gML NB")
        mlflow.end_run()
        mlflow.start_run()
        nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
        model = nb.fit(dfTrain)
        predictions = model.transform(dfTest)
        metrics = ["accuracy", "f1"]
        result = []
        for metric in metrics:
            evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName=metric)
            v = evaluator.evaluate(predictions)
            mlflow.log_metric(metric, v)
            # print("  {}: {}".format(metric,v))
            temp = [metric,v]
            result.append(temp)
        mlflow.spark.log_model(model, "nbModel")
        return result

class dtClassifier:
    def dtModel(self, dfTrain, dfTest, seed):
        client = mlflow.tracking.MlflowClient()
        mlflow.set_experiment("gML DT")
        mlflow.end_run()
        mlflow.start_run()
        dt = DecisionTreeClassifier(featuresCol='features',
                                    labelCol='label',
                                    predictionCol='prediction',
                                    probabilityCol='probability',
                                    rawPredictionCol='rawPrediction',
                                    maxDepth=5,
                                    maxBins=32,
                                    minInstancesPerNode=1,
                                    minInfoGain=0.0,
                                    maxMemoryInMB=256,
                                    cacheNodeIds=False,
                                    checkpointInterval=10,
                                    impurity='gini',
                                    seed=None,)
        model = dt.fit(dfTrain)
        predictions = model.transform(dfTest)
        # predictions.show()
        metrics = ["accuracy", "f1"]
        result = []
        for metric in metrics:
            evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                          metricName=metric)
            v = evaluator.evaluate(predictions)
            mlflow.log_metric(metric, v)
            temp = [metric, v]
            result.append(temp)
        mlflow.spark.log_model(model, "dtModel")
        return result

class rfClassifier:
    def rfModel(self, dfTrain, dfTest, seed):
        client = mlflow.tracking.MlflowClient()
        mlflow.set_experiment("gML RF")
        mlflow.end_run()
        mlflow.start_run()
        rf = RandomForestClassifier(featuresCol='features',
                                    labelCol='label',
                                    predictionCol='prediction',
                                    probabilityCol='probability',
                                    rawPredictionCol='rawPrediction',
                                    maxDepth=5,
                                    maxBins=32,
                                    minInstancesPerNode=1,
                                    minInfoGain=0.0,
                                    maxMemoryInMB=256,
                                    cacheNodeIds=False,
                                    checkpointInterval=10,
                                    impurity='gini',
                                    numTrees=20,
                                    featureSubsetStrategy='auto',
                                    seed=None,
                                    subsamplingRate=1.0,)
        model = rf.fit(dfTrain)
        predictions = model.transform(dfTest)
        metrics = ["accuracy", "f1"]
        result = []
        for metric in metrics:
            evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                          metricName=metric)
            v = evaluator.evaluate(predictions)
            mlflow.log_metric(metric, v)
            temp = [metric, v]
            result.append(temp)
        mlflow.spark.log_model(model, "rfModel")
        return result

class gbtClassifier:
    def gbtModel(self, dfTrain, dfTest, seed):
        client = mlflow.tracking.MlflowClient()
        mlflow.set_experiment("gML GBT")
        mlflow.end_run()
        mlflow.start_run()
        gbt = GBTClassifier(labelCol="label",
                            featuresCol="features",
                            predictionCol='prediction',
                            maxDepth=5,
                            maxBins=32,
                            minInstancesPerNode=1,
                            minInfoGain=0.0,
                            maxMemoryInMB=256,
                            cacheNodeIds=False,
                            checkpointInterval=10,
                            lossType='logistic',
                            maxIter=20,
                            stepSize=0.1,
                            seed=None,
                            subsamplingRate=1.0,
                            featureSubsetStrategy='all',)
        model = gbt.fit(dfTrain)
        predictions = model.transform(dfTest)
        metrics = ["accuracy", "f1"]
        result = []
        for metric in metrics:
            evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                          metricName=metric)
            v = evaluator.evaluate(predictions)
            mlflow.log_metric(metric, v)
            temp = [metric, v]
            result.append(temp)
        mlflow.spark.log_model(model, "gbtModel")
        return result
