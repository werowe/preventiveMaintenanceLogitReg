import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.sql.SQLContext
 import java.io.DataOutputStream
import java.io.BufferedWriter
import org.apache.hadoop.fs.FSDataOutputStream
 import java.io.OutputStreamWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.util.Date
import java.text.SimpleDateFormat

object generateData {
def generateData (mean: Double, stddev: Double, max: Double, min:Double) : Double = {

var x:NormalDistribution = new NormalDistribution(stddev,mean)
var y:Double = x.sample()
while( (y >= max) || (y <= min) ) {
   y = x.sample()
   } 
 return y
}

def createData(x: org.apache.spark.sql.DataFrame) : Double = {

var y:Array[org.apache.spark.sql.Row] = x.collect();
var mean:Double = y(1)(1).toString.toDouble;
var stddev:Double = y(2)(1).toString.toDouble;
var min:Double = y(3)(1).toString.toDouble;
var max:Double = y(4)(1).toString.toDouble;
return generateData(mean,stddev,max,min);

}

def main(args: Array[String]): Unit = {
 val conf = new SparkConf().setAppName("lr")
 val sc = new SparkContext(conf)
var records:Int = args(0).toInt;
var hdfsCoreSite = args(1)
var file = args(2)

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter",";").load(file)
var header = "lifetime;broken;pressureInd;moistureInd;temperatureInd;team;provider"

val date = new Date()
var dformat:SimpleDateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
val csvFile =  "/maintenance/" + dformat.format(date) + ".csv"

println("writing to " + csvFile)

val fs = {
    val conf = new Configuration()
conf.addResource(new Path(hdfsCoreSite))
    FileSystem.get(conf)
  }

val dataOutputStream: FSDataOutputStream = fs.create(new Path(csvFile))
val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
     
println(header)
bw.write(header + "\n")

val r = scala.util.Random
var i:Int = 0
while (i < records ) {

var pressureInd =  createData(df.describe("pressureInd"))
var moistureInd =  createData(df.describe("moistureInd"))
var temperatureInd =  createData(df.describe("temperatureInd"))
var lifetime =  createData(df.describe("lifetime"))

var str = lifetime + ";" + "0" + ";" + pressureInd + ";" + moistureInd + ";" + temperatureInd +";" + r.nextInt(100) + ";" + "Ford F-750"

println (str)
bw.write(str + "\n")
   
i = i + 1
}
bw.close

}
}
