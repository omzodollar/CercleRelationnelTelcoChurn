package com.crtchurn.common

import java.io.File

import org.apache.spark.sql.DataFrame

object Common {
  def logDataFrame (df : DataFrame, count : Integer = 5, truncate : Boolean = false) : Unit = {
    df.schema.fields.map(_.toString())
    df.printSchema()
    df.show(count, truncate)
  }
  def saveDfToCsv(df: DataFrame, tsvOutput: String,
                  sep: String = ",", header: Boolean = false): Unit = {
    val tmpParquetDir = "Data"
    df.repartition(1).write.
      format("com.databricks.spark.csv").
      option("header", header.toString).
      option("delimiter", sep).
      save(tmpParquetDir)

    val dir = new File(tmpParquetDir)
    val tmpTsvFile = tmpParquetDir + File.separatorChar + "part-00000"
    (new File(tmpTsvFile)).renameTo(new File(tsvOutput))
  }

}
