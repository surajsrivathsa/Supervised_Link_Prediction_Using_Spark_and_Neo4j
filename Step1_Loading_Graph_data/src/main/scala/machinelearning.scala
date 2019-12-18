import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml._
import org.apache.spark.ml.feature.{LabeledPoint, VectorAssembler}
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator


class machinelearning(spark: SparkSession) {

  def ml: Unit = {
    val mlschema = StructType(Array(StructField("authorId1", LongType, nullable = false), StructField("authorId2", LongType, nullable = false), StructField("label", StringType, nullable = false),StructField("common_neighbours", DoubleType, nullable = false),StructField("pagerankscore1", DoubleType, nullable = false), StructField("pagerankscore2", DoubleType, nullable = false), StructField("preferential_attachment_score1", DoubleType, nullable = false), StructField("preferential_attachment_score2", DoubleType, nullable = false)))
    var mldf = spark.read.schema(mlschema).format("csv").option("header", "false").option("mode", "DROPMALFORMED").option("inferSchema", "false").load("/Users/surajshashidhar/Desktop/graphml_10lkh/featureset4/featureset4.csv");
    mldf.show(10);

    mldf.createOrReplaceTempView("table")
    var inputdata = spark.sql("select authorId1, authorId2,  common_neighbours,pagerankscore1,pagerankscore2,preferential_attachment_score1 ,preferential_attachment_score2, cast((CASE WHEN label = 'Yes' THEN 1 ELSE 0 END) as double) as label from table");
    inputdata.show(10);

    val assembler = new VectorAssembler().setInputCols(Array("common_neighbours", "pagerankscore1", "pagerankscore2", "preferential_attachment_score1", "preferential_attachment_score2")).setOutputCol("features")
    var data_2 = assembler.transform(inputdata)

    val Array(training, test) = data_2.randomSplit(Array(0.7, 0.3), seed = 1234L);
    val model = new NaiveBayes().fit(training);

    // Select example rows to display.
    val predictions = model.transform(test)
    predictions.show()


    predictions.createOrReplaceTempView("predictions");
    spark.sql("select count(*),label,prediction from predictions group by label,prediction").show(10);


    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")
    val areaUnderROC = evaluator.evaluate(predictions)
    println(s"Test set area under roc = $areaUnderROC")

    val evaluator1 = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")





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
