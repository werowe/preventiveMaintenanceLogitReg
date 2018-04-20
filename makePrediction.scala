package com.bmc.lr

import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext


object makePrediction {

def main(args: Array[String]): Unit = {
 val conf = new SparkConf().setAppName("lr")
 val sc = new SparkContext(conf)
var modelFile = args(1);
var file = args(0);

val sqlContext = new SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter",";").load(file)

val featureCols =  Array("lifetime", "pressureInd", "moistureInd", "temperatureInd")

val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

val labelIndexer = new StringIndexer().setInputCol("broken").setOutputCol("label")

val df2 = assembler.transform(df)

val df3 = labelIndexer.fit(df2).transform(df2)

val model = LogisticRegressionModel.load(modelFile)

val predictions = model.transform(df3)

var df4 = predictions.select ("team", "provider", "pressureInd", "moistureInd", "temperatureInd", "label", "prediction")

val df5 = df4.filter("prediction=1")

df5.show()

import java.util.Date
import java.text.SimpleDateFormat
val date = new Date()
var dformat:SimpleDateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
val csvFile = "hdfs://localhost:9000/maintenance/" + dformat.format(date) + ".csv"
df5.write.format("com.databricks.spark.csv").option("header", "true").save(csvFile)

}
}
