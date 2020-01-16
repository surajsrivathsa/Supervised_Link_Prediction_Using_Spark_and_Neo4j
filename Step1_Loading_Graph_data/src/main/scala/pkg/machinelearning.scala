package pkg

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, _}


class machinelearning(spark: SparkSession) {

  def ml: Unit = {
    val mlschema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false)  ,StructField("year", IntegerType, nullable = true) , StructField("label", IntegerType, nullable = false),StructField("pagerankscore1", DoubleType, nullable = false), StructField("pagerankscore2", DoubleType, nullable = false), StructField("preferential_attachment_score1", DoubleType, nullable = false), StructField("preferential_attachment_score2", DoubleType, nullable = false)))

    //train
    var mldf = spark.read.schema(mlschema).format("csv").option("sep","|").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema", "false").load("/Users/surajshashidhar/Desktop/graphml_10lkh/training_features.csv");
    mldf.show(10);

    mldf.createOrReplaceTempView("table")
    var inputdata = spark.sql("select distinct authorId1, authorId2, pagerankscore1,pagerankscore2,preferential_attachment_score1 ,preferential_attachment_score2, label from table");
    inputdata.show(10);

    val assembler = new VectorAssembler().setInputCols(Array("pagerankscore1", "pagerankscore2", "preferential_attachment_score1", "preferential_attachment_score2")).setOutputCol("features")
    var data_2 = assembler.transform(inputdata)

    val Array(training, test) = data_2.randomSplit(Array(0.7, 0.3), seed = 1234L);
    val model = new NaiveBayes().fit(data_2);

    //test
    var mldf1 = spark.read.schema(mlschema).format("csv").option("sep","|").option("header", "true").option("mode", "DROPMALFORMED").option("inferSchema", "false").load("/Users/surajshashidhar/Desktop/graphml_10lkh/validation_features.csv");
    mldf1.show(10);

    mldf1.createOrReplaceTempView("table1")
    var inputdata1 = spark.sql("select distinct authorId1, authorId2, pagerankscore1,pagerankscore2,preferential_attachment_score1 ,preferential_attachment_score2, label from table1");
    inputdata1.show(10);

    val assembler1 = new VectorAssembler().setInputCols(Array("pagerankscore1", "pagerankscore2", "preferential_attachment_score1", "preferential_attachment_score2")).setOutputCol("features")
    var data_3 = assembler1.transform(inputdata1)


    // Select example rows to display.
    val predictions = model.transform(data_3)
    predictions.show()

    predictions.createOrReplaceTempView("predictions");
    //var outputdata = spark.sql("select authorId1, authorId2, common_neighbours,pagerankscore1,pagerankscore2,preferential_attachment_score1 ,preferential_attachment_score2,  label as expected_label,  prediction as prediction_label from predictions")
    //outputdata.write.option("header", "true").csv("/Users/surajshashidhar/Desktop/grpahml_10lkh/predictions")

    spark.sql("select count(*),label,prediction from predictions group by label,prediction").show(10);


    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
    val areaUnderROC = evaluator.evaluate(predictions)
    println(s"Test set area under roc = $areaUnderROC")

    val evaluator1 = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")


    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(inputdata)

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data_2);

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data_2.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(10)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val modelrf = pipeline.fit(data_2)

    // Make predictions.
    val predictionsrf = modelrf.transform(data_3)

    // Select example rows to display.
    predictionsrf.select("predictedLabel", "label", "features").show(5)

    predictionsrf.createOrReplaceGlobalTempView("tmp1");

    var new_predictionsrf = spark.sql("select cast(predictedLabel as DOUBLE) predictedLabel, cast(label as DOUBLE) as label, features from global_temp.tmp1");
    spark.sql("select count(*), label, predictedLabel from global_temp.tmp1 group by  label, predictedLabel").show(10)
    val evaluator2 = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("predictedLabel").setMetricName("accuracy")
    val accuracy2 = evaluator2.evaluate(new_predictionsrf)
    println(s"Test set accuracy = $accuracy2")
    //getAs[org.apache.spark.ml.linalg.SparseVector]("features").toDense

    //var training1 = training.rdd.map(row => LabeledPoint(row.getAs[Double]("expectedvalue"),row.getAs[org.apache.spark.ml.linalg.Vector]("features") ))
    //var testing1 = test.rdd.map(row => LabeledPoint(row.getAs[Double]("expectedvalue"),row.getAs[org.apache.spark.ml.linalg.Vector]("features") ))

    //var model = NaiveBayes.train(training1,1.0,"multinomial");


  }

}
/*
val Array(training, test) = data_2.randomSplit(Array(0.6, 0.4))
var training1 = training.rdd.map(row => LabeledPoint(row.getAs[Double]("expectedvalue"),row.getAs[org.apache.spark.ml.linalg.Vector]("features") ))
var testing1 = test.rdd.map(row => LabeledPoint(row.getAs[Double]("expectedvalue"),row.getAs[org.apache.spark.ml.linalg.Vector]("features") ))
val model = new NaiveBayes().fit(training1)

val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
val accuracy = evaluator.evaluate(predictions)
println(s"Test set accuracy = $accuracy")

val predictionAndLabels = test.map { case LabeledPoint(label, features) => val prediction = model.predict(features)(prediction, label)}

 */
