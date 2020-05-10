package com.spark.drivers

import com.spark.assignment2.enums.SteamTable
import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVFileDriver extends DataDriver {

  /**
    * Main method intended to be called from `spark-submit`.
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // do nothing
  }

  /**
    * Loads data from the specified table using the given spark session.
    * @param sparkSession
    * @param steamTable
    * @return
    */
  override def readTableData(sparkSession: SparkSession, steamTable: SteamTable.Value): DataFrame = {
    val csvReadOptions = Map("inferSchema" -> true.toString, "header" -> true.toString)
    sparkSession.read.options(csvReadOptions).csv(steamTable.CSVFilePath)
  }
}
