name := "CercleRelationnelTelcoChurn"

version := "0.1"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-graphx" % "2.4.0" ,
  "com.typesafe" % "config" % "1.3.1",
  "com.typesafe.scala-logging" %% "scala-logging-api" % "2.1.2",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "org.apache.commons" % "commons-csv" % "1.4",
   "org.neo4j.driver" % "neo4j-java-driver" % "1.7.2" ,
  "org.neo4j" % "openCypher-frontend-1" % "3.4.12" % Test
)
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
resolvers+= "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/graphframes/graphframes"
libraryDependencies += "graphframes" % "graphframes" % "0.7.0-spark2.4-s_2.11"


