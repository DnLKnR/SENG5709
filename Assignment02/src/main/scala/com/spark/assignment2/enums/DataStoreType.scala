package com.spark.assignment2.enums

import com.spark.drivers.{CSVFileDriver, DataDriver, ParquetDriver, SqlServerDriver}

/**
  * The data storage technology types
  */
object DataStoreType extends Enumeration {
  val CSV, SqlServer, Parquet = Value

  /**
    * Extension functions on this enum
    *
    * Based off this stackoverflow answer:
    *    https://stackoverflow.com/a/4348550
    * @param dataStoreType
    */
  class DataStoreTypeExtensions(dataStoreType: Value) {

    /**
      * Gets the driver for the specified data store.
      * @return
      */
    def Driver: DataDriver = dataStoreType match {
      case DataStoreType.CSV       => CSVFileDriver
      case DataStoreType.SqlServer => SqlServerDriver
      case DataStoreType.Parquet   => ParquetDriver
    }
  }
  implicit def value2DataStoreType(dataStoreType: Value) = new DataStoreTypeExtensions(dataStoreType)
}
