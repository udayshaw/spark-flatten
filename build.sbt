name := "spark-flatten"
version := "1.0"

mainClass in (assembly) := Some("flatten")
scalaVersion := "2.12.15"
assemblyJarName in assembly := "spark-flatten.jar"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided"
