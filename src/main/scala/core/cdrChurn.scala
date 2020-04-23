
package com.crtchurn.core
import java.io.File
import org.apache.spark.sql.functions.{unix_timestamp, to_date}

import com.crtchurn.CRTApp
import com.crtchurn.CRTApp._spark
import org.apache.spark.sql.DataFrame
import _spark.implicits._
object cdrChurn {

  def readCSVFile() : DataFrame = {
    val path = CRTApp._configFile.getString("CRTCHURN.SOURCES.CDR.PATH")
    val cdrFile = CRTApp._spark.read.options(Map("inferSchema" -> "true", "sep" -> "|", "header" -> "true")).csv(path)

    cdrFile.select("CALLING_NBR","CALLED_NBR","EVENT_BEGIN_TIME","DURATION","CALL_TYPE","CALLED_PREFIX","TIME_NUMBER","SUBS_ID","CHARGE1","CHARGE2","CHARGE3","CHARGE4","PRICE_ID1","PRICE_ID2","PRICE_ID3","PRICE_ID4","NETWORK_TYPE","CALLING_AREA_CODE","SUBS_ID")
  }
  def editFile(): DataFrame= {
    readCSVFile().createOrReplaceTempView("churnTable")
    val query =
      s"""
         |SELECT *
         |from churnTable
       """.stripMargin

    CRTApp._spark.sql(query)

  }
  def selectUsefulColumn () : DataFrame =  {
    val usefulData = readCSVFile().select("CALLING_NBR", "EVENT_BEGIN_TIME","CALLED_NBR","DURATION","CALL_TYPE",
      "CALLED_PREFIX","TIME_NUMBER","SUBS_ID","CHARGE1","CHARGE2","CHARGE3","CHARGE4",
        "PRICE_ID1","PRICE_ID2","PRICE_ID3","PRICE_ID4","NETWORK_TYPE","CALLING_AREA_CODE")
    usefulData
  }





}
