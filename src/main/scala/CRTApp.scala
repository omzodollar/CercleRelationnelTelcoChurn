package com.crtchurn
import java.io.File
import java.lang.System.setProperty

import com.crtchurn.core.cdrChurn
import com.crtchurn.core.Graph
import com.crtchurn.common.Common
import com.crtchurn.core.Cdr
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import org.neo4j.spark.{Neo4j, Neo4jConfig, Neo4jDataFrame, Neo4jGraph}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame


object CRTApp extends App {
  setProperty("hadoop.home.dir", """c:\winutils""")
  val  _configFile = ConfigFactory.parseFile(new File("src/main/resources/crtchurn.conf"))
  val _log : Logger = LoggerFactory.getLogger(CRTApp.getClass)

  val config = new SparkConf()
  config.set(Neo4jConfig.prefix + "url", "bolt://localhost:11001")
  config.set(Neo4jConfig.prefix + "user", "neo4j")
  config.set(Neo4jConfig.prefix + "password", "passer")


  val _spark : SparkSession = SparkSession.
                                            builder()
                                            .master("local[*]").
                                            appName(_configFile.getString("CRTCHURN.APPS.NAME")).
                                            config(config).
                                            getOrCreate()
  //cdrChurn.readCSVFile().take(100).foreach(println)

  cdrChurn.selectUsefulColumn().show(5)
 // Common.logDataFrame(Cdr.totalDurationCall())
  //Cdr.totalNumberCall().show(5)
 // cdrChurn.saveDfToCsv(cdrChurn.selectUsefulColumn(),"churn.csv","|",true)
 //dr.number_call_peer_sub().show(100)

  _spark.sparkContext.setLogLevel("WARN")
  val neo = Neo4j(_spark.sparkContext)

  val vertices = Graph.createVertice()
  val edges= Graph.createEdge()

  val g = GraphFrame(vertices,edges)
  vertices.cache()
  g.vertices.show(10)
  g.edges.show(10)
  //GraphToNeo4j.createGraph()
  g.vertices.count()
 // Graph.saveGraph()
  val sc=_spark.sparkContext
  //Neo4jDataFrame.mergeEdgeList(sc,Cdr.totalDurationCall().na.drop(),("MSSIDN",Seq("CALLING_NBR")),("Nombre_Appel",Seq("APPEL_CNT","DUREE_APPEL")),("MSSIDN",Seq("CALLED_NBR")))
  val nbr = Window.partitionBy("CALLING_NBR")
  Cdr.totalDurationCall().withColumn("sum",sum("APPEL_CNT")over nbr).show(100)



}


