import com.databricks.spark.csv
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.LogisticRegressionModel

var file = "hdfs://localhost:9000/maintenance/maintenance_data.csv"

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter",";").load(file)

val featureCols =  Array("lifetime", "pressureInd", "moistureInd", "temperatureInd")

val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

val labelIndexer = new StringIndexer().setInputCol("broken").setOutputCol("label")

val df2 = assembler.transform(df)

val df3 = labelIndexer.fit(df2).transform(df2)

val model = LogisticRegressionModel.load("hdfs://localhost:9000/maintenance/maintenance_model")

val predictions = model.transform(df3)

predictions.select ("team", "provider", "features", "label", "prediction")

var df4 = predictions.select ("team", "provider", "features", "label", "prediction")

val model = LogisticRegression.load("hdfs://localhost:9000/maintenance/maintenance_model")

val df5 = df4.filter("prediction=1")

df5.show()

val df6 = df5.select("team", "provider")

df6.show()

import java.util.Date
import java.text.SimpleDateFormat
val date = new Date()
var df:SimpleDateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
val file = "hdfs://localhost:9000/maintenance/" + df.format(date) + ".csv"

df6.write.format("com.databricks.spark.csv").option("header", "true").save(file)
