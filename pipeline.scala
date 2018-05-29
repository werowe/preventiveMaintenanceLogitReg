import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.databricks.spark.csv
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.LogisticRegressionModel
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.ml.{Pipeline, PipelineModel}

var file = "hdfs://localhost:9000/maintenance/maintenance_data.csv";
val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter",";").load(file)
df.show()

val featureCols =  Array("lifetime", "pressureInd", "moistureInd", "temperatureInd")

val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

val labelIndexer = new StringIndexer().setInputCol("broken").setOutputCol("label")

val df2 = assembler.transform(df)

val df3 = labelIndexer.fit(df2).transform(df2)

val lr = new LogisticRegression()

val pipeline = new Pipeline().setStages(Array(lr))

val model = pipeline.fit(df3)

var predictFile = "hdfs://localhost:9000/maintenance/2018.04.20.15.48.54.csv"

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter",";").load(file)

val featureCols =  Array("lifetime", "pressureInd", "moistureInd", "temperatureInd")

val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

val labelIndexer = new StringIndexer().setInputCol("broken").setOutputCol("label")

val df2 = assembler.transform(df)

val df3 = labelIndexer.fit(df2).transform(df2)

val predictions = model.transform(df3)
   
var df4 = predictions.select ("team", "provider", "pressureInd", "moistureInd", "temperatureInd", "label", "prediction", "probability")

df4.show()

