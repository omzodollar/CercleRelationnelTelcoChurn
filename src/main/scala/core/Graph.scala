package com.crtchurn.core
import com.crtchurn.CRTApp.{_configFile, config, g}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.neo4j.spark.Neo4j

object Graph {
  def createVertice(): DataFrame ={
    Cdr.totalDurationCall().toDF("id","CALLED_NBR","DUREE_APPEL","APPEL_CNT")
  }
  def  createEdge(): DataFrame ={
    Cdr.totalDurationCall().toDF("src","dst","DUREE_APPEL","APPEL_CNT")

  }
  def saveGraph():Unit ={
    val config = new SparkConf()
    config.setAppName(_configFile.getString("CRTCHURN.APPS.NAME"))
    config.set("spark.driver.allowMultipleContexts", "true")
    config.setMaster("local")
    val sc = new SparkContext(config)
    val neo = Neo4j(sc)
    neo.saveGraph(g.toGraphX,"Subscriber")
  }
}
