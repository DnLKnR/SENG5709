package com.spark.drivers

import java.sql.DriverManager
import com.spark.assignment2.enums.SteamTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SqlServerDriver extends DataDriver {

  /**
    * Main method intended to be called from `spark-submit`.
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val distributedSparkSession =
      SparkSession.builder().appName("Testing Example").getOrCreate()

    for (steamTable: SteamTable.Value <- SteamTable.values) {
      val data = SqlServerDriver.readTableData(distributedSparkSession, steamTable)
      data.write.mode(SaveMode.Overwrite).parquet(steamTable.ParquetFilePath)
    }
  }

  /**
    * Loads data from the specified table using the given spark session.
    * @param spark
    * @param steamTable
    * @return
    */
  override def readTableData(spark: SparkSession, steamTable: SteamTable.Value): DataFrame = {
    // had to download sql server driver JAR and add it to project from here:
    // https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server
    val driverClass: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    // make sure we have the proper class
    Class.forName(driverClass)

    // connection properties
    val username = "sparktest"
    val password = "password1"
    val hostname = "127.0.0.1"
    val port = "1433"
    val database = "steam"
    val url = s"jdbc:sqlserver://${hostname}:${port};databaseName=${database};user=${username};password=${password};"

    // checking to see if we can actually connect
    val connection = DriverManager.getConnection(url)
    connection.isClosed()
    connection.close()

    // read data from the database
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", steamTable.SqlTableName)
      .option("driver", driverClass)
      .option("inferSchema", true.toString())
      .load()
  }
}
