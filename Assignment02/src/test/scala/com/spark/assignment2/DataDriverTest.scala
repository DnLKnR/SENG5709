package com.spark.assignment2

import com.spark.assignment2.enums.SteamTable
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class DataDriverTest extends AnyFunSuite {
  // Build a `SparkSession` to be used during tests
  val spark =
    SparkSession
      .builder()
      .appName("DataFrame Examples")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .config("spark.executor.instances", "3") // 3 executors
      .config("spark.executor.cores", "1") // 1 core each
      .getOrCreate()

  test("Checks App Info Row Count") {
    val steamTable: SteamTable.Value = SteamTable.AppInfo
    // load the dataframe
    val dfAppInfo = Assignment2.DataStore.Driver.readTableData(spark, steamTable)
    // check row count
    assert(dfAppInfo.count === steamTable.ExpectedRows)
    // secondary dimension check to make sure commas are encoded properly
    assert(dfAppInfo.columns.size === steamTable.ExpectedColumns)
  }

  test("Checks Games 1 Row Count") {
    val steamTable: SteamTable.Value = SteamTable.Games1
    // load the dataframe
    val dfSteamTable = Assignment2.DataStore.Driver.readTableData(spark, steamTable)
    // check row count
    assert(dfSteamTable.count === steamTable.ExpectedRows)
    // secondary dimension check to make sure commas are encoded properly
    assert(dfSteamTable.columns.size === steamTable.ExpectedColumns)
  }

  test("Checks Games 2 Row Count") {
    val steamTable: SteamTable.Value = SteamTable.Games2
    // load the dataframe
    val dfSteamTable = Assignment2.DataStore.Driver.readTableData(spark, steamTable)
    // check row count
    assert(dfSteamTable.count === steamTable.ExpectedRows)
    // secondary dimension check to make sure commas are encoded properly
    assert(dfSteamTable.columns.size === steamTable.ExpectedColumns)
  }

  test("Checks Player Summaries Row Count") {
    val steamTable: SteamTable.Value = SteamTable.PlayerSummaries
    // load the dataframe
    val dfSteamTable = Assignment2.DataStore.Driver.readTableData(spark, steamTable)
    // check row count
    assert(dfSteamTable.count === steamTable.ExpectedRows)
    // secondary dimension check to make sure commas are encoded properly
    assert(dfSteamTable.columns.size === steamTable.ExpectedColumns)
  }
}
