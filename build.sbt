name         := "lr"
version      := "1.0"
organization := "com.bmc"

scalaVersion := "2.11.8"
mainClass := Some("com.bmc.lr")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.3.0" % "provided", "com.databricks" %% "spark-csv" % "1.5.0", "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided", "org.apache.spark" %% "spark-mllib" % "2.3.0" % "provided")
resolvers += Resolver.mavenLocal
