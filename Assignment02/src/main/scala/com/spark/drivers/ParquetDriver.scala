package com.spark.drivers

import com.spark.assignment2.enums.SteamTable
import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetDriver extends DataDriver {

  override def readTableData(sparkSession: SparkSession, steamTable: SteamTable.Value): DataFrame = {
    sparkSession.read.parquet(steamTable.ParquetFilePath)
  }
}
