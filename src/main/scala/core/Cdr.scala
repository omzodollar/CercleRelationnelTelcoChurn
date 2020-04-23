package com.crtchurn.core
import org.apache.spark.sql.DataFrame
import com.crtchurn.CRTApp
import com.crtchurn.core.cdrChurn.readCSVFile
import java.io._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.count


object Cdr {
  def totalDurationCall(): DataFrame= {
    cdrChurn.readCSVFile().createOrReplaceTempView("callInformation")
    val duration =
        s"""
           |SELECT CALLING_NBR, CALLED_NBR, SUM(cast(DURATION AS Int)) AS DUREE_APPEL, count(*) AS APPEL_CNT
           |from callInformation
           |GROUP BY CALLING_NBR, CALLED_NBR
         """.stripMargin
    CRTApp._spark.sql(duration)

    }
  //nombre d'appel entre deux personnes
  def totalNumberCall(): DataFrame= {
    //val totalOfCall= cdrChurn.readCSVFile().select("CALLING_NBR","CALLED_NBR","DURATION")
    cdrChurn.readCSVFile().createOrReplaceTempView("callInformation")
    val duration =
      s"""
         |SELECT SUM(CALLING_NBR)
         |from callInformation GROUP BY CALLING_NBR,CALLED_NBR
         """.stripMargin
    CRTApp._spark.sql(duration)

  }
  def op_indicator(): DataFrame= {
    readCSVFile().createOrReplaceTempView("operateur")
    val query =
      s"""
         |SELECT DISTINCT CALLED_PREFIX
         |from operateur
       """.stripMargin

    CRTApp._spark.sql(query)

  }
    def number_call_peer_sub(): DataFrame= {
   /* val somme = Window.partitionBy("CALLING_NBR")

      readCSVFile().withColumn("Nombre_Appel", count("CALLING_NBR").over(somme) ).orderBy("SUBS_ID")*/
    val somme=readCSVFile().groupBy("CALLING_NBR").agg(count("CALLING_NBR").as("Nombre_Appel"))
      somme
    }

}
