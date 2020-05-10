package com.spark.assignment2

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.spark.assignment2.enums.{DataStoreType, SteamTable}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object Assignment2 {

  private val timestampFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm")

  /**
    * Driver used to obtain data.
    */
  val DataStore: DataStoreType.Value = DataStoreType.Parquet;

  /**
    * Helper function to print out the contents of an RDD
    * @param label Label for easy searching in logs
    * @param theRdd The RDD to be printed
    * @param limit Number of elements to print
    */
  private def printRdd[_](label: String, theRdd: RDD[_], limit: Integer = 20) = {
    val limitedSizeRdd = theRdd.take(limit)
    println(s"""$label ${limitedSizeRdd.toList.mkString(",")}""")
  }

  /**
    * What game has the highest average play time?
    */
  def problem1(spark: SparkSession): (Int, String, Double) = {
    val dfGames2: DataFrame = DataStore.Driver.readTableData(spark, SteamTable.Games2).toDF()
    val highestAverageGame: Row = dfGames2
      .groupBy(col("appid"))
      .agg(avg("playtime_forever").as("avg_playtime_forever"))
      .orderBy(desc("avg_playtime_forever"))
      .first()

    val averagePlaytime = highestAverageGame.getAs[Double]("avg_playtime_forever")
    val appid = highestAverageGame.getAs[Int]("appid")
    val appTitle = DataStore.Driver.getAppInfo(spark, appid).Title
    (appid, appTitle, averagePlaytime)
  }

  /**
    * What game has the highest amount of users in the dataset?
    */
  def problem2(spark: SparkSession): (Int, String, Long) = {
    val dfGames2: DataFrame = DataStore.Driver.readTableData(spark, SteamTable.Games2).toDF()
    val mostPlayersGame: Row =
      dfGames2.groupBy(col("appid")).agg(count("steamid").as("player_count")).orderBy(desc("player_count")).first()
    // extract data from row
    val appid: Int = mostPlayersGame.getAs[Int]("appid")
    val totalPlayers: Long = mostPlayersGame.getAs[Long]("player_count")
    val appTitle: String = DataStore.Driver.getAppInfo(spark, appid).Title
    (appid, appTitle, totalPlayers)
  }

  /**
    * What user has the highest play time for a single game?
    */
  def problem3(spark: SparkSession): (Int, String, Long, String, Int) = {
    val dfGames2: DataFrame = DataStore.Driver.readTableData(spark, SteamTable.Games2).toDF()
    val mostPlaytimeForAUser: Row = dfGames2.orderBy(desc("playtime_forever")).head()

    val appid: Int = mostPlaytimeForAUser.getAs[Int]("appid")
    val steamid: Long = mostPlaytimeForAUser.getAs[Long]("steamid")
    val playtime: Int = mostPlaytimeForAUser.getAs[Int]("playtime_forever")
    val appTitle: String = DataStore.Driver.getAppInfo(spark, appid).Title
    val playerName: String = DataStore.Driver.getPlayer(spark, steamid).personaname
    (appid, appTitle, steamid, playerName, playtime)
  }

  /**
    * What user has the highest total play time?
    */
  def problem4(spark: SparkSession): (Long, String, Long) = {
    val dfGames2: DataFrame = DataStore.Driver.readTableData(spark, SteamTable.Games2).toDF()
    val mostPlayersGame: Row = dfGames2
      .groupBy(col("steamid"))
      .agg(sum("playtime_forever").as("total_playtime"))
      .orderBy(desc("total_playtime"))
      .head()
    // extract data from row
    val steamid: Long = mostPlayersGame.getAs[Long]("steamid")
    val totalPlaytime: Long = mostPlayersGame.getAs[Long]("total_playtime")
    val playerName: String = DataStore.Driver.getPlayer(spark, steamid).personaname
    (steamid, playerName, totalPlaytime)
  }

  /**
    * What game has the highest ratio of [number of hours played] to [average hour per user]?
    */
  def problem5(spark: SparkSession): (Int, String, Double) = {
    val dfGames2: DataFrame = DataStore.Driver.readTableData(spark, SteamTable.Games2).toDF()

    val dfAggGames2 = dfGames2
      .groupBy(col("appid"))
      .agg(sum("playtime_forever").as("total_playtime"), avg("playtime_forever").as("avg_playtime"));

    val highestRatioGameRow: Row = dfAggGames2
      .filter(col("avg_playtime").gt(lit(0)))
      .select(col("appid"), col("total_playtime").divide(col("avg_playtime")).as("playtime_ratio"))
      .orderBy(desc("playtime_ratio"))
      .head()

    // extract data from row
    val appid: Int = highestRatioGameRow.getAs[Int]("appid")
    val playtimeRatio: Double = highestRatioGameRow.getAs[Double]("playtime_ratio")
    val appTitle: String = DataStore.Driver.getAppInfo(spark, appid).Title
    (appid, appTitle, playtimeRatio)
  }

  /**
    * What is the average amount of users per Required Age?
    */
  def problem6(spark: SparkSession): DataFrame = {
    val dfGameInfo: DataFrame = DataStore.Driver.readTableData(spark, SteamTable.AppInfo).toDF()
    val dfGames2: DataFrame = DataStore.Driver.readTableData(spark, SteamTable.Games2).toDF()

    val dfUsersPerRequiredAge: DataFrame = dfGames2
      .join(dfGameInfo, dfGames2("appid") === dfGameInfo("appid"), "inner")
      .filter(col("Type").like("game"))
      .groupBy(dfGameInfo("Required_Age"))
      .agg(count(dfGames2("steamid")).as("user_count"))

    dfUsersPerRequiredAge
  }

  /**
    * What game had the biggest increase in average playtime from the earliest to latest retrieval date?
    */
  def problem7(spark: SparkSession): (Int, String, Double) = {
    val dfAvgGames1: DataFrame = DataStore.Driver
      .readTableData(spark, SteamTable.Games1)
      .toDF()
      .groupBy(col("appid"))
      .agg(avg("playtime_forever").as("avg_playtime"))
    val dfAvgGames2: DataFrame = DataStore.Driver
      .readTableData(spark, SteamTable.Games2)
      .toDF()
      .groupBy(col("appid"))
      .agg(avg("playtime_forever").as("avg_playtime"))

    // since a game might have been release games 1 and 2 datasets
    // coalesce non-existent older games (not released yet)
    val avgGames1Col: Column = coalesce(dfAvgGames1("avg_playtime"), lit(0))
    val avgGames2Col: Column = dfAvgGames2("avg_playtime")
    val avgDeltaCol: Column = abs(avgGames2Col.minus(avgGames1Col)).as("avg_playtime_delta")

    val maxAvgDeltaRow: Row = dfAvgGames2
      .join(dfAvgGames1, dfAvgGames2("appid") === (dfAvgGames1("appid")), "left")
      .select(dfAvgGames2("appid"), avgDeltaCol)
      .orderBy(desc("avg_playtime_delta"))
      .head()

    // extract data from row
    val appid: Int = maxAvgDeltaRow.getAs[Int]("appid")
    val avgPlaytimeDelta: Double = maxAvgDeltaRow.getAs[Double]("avg_playtime_delta")
    val appTitle: String = DataStore.Driver.getAppInfo(spark, appid).Title
    (appid, appTitle, avgPlaytimeDelta)
  }

  /**
    * Returns the average playtime delta of all games that were released before the initial dataset capture
    * and of all games after the initial dataset capture.
    * @param spark
    * @return (Average Playtime Delta Before, Average Playtime Delta After)
    */
  def problem8(spark: SparkSession): (Double, Double) = {
    val dfAvgGames1: DataFrame = DataStore.Driver
      .readTableData(spark, SteamTable.Games1)
      .toDF()
      .groupBy(col("appid"))
      .agg(avg("playtime_forever").as("avg_playtime"))
    val dfAvgGames2: DataFrame = DataStore.Driver
      .readTableData(spark, SteamTable.Games2)
      .toDF()
      .groupBy(col("appid"))
      .agg(avg("playtime_forever").as("avg_playtime"))

    // since a game might have been release games 1 and 2 datasets
    // coalesce non-existent older games (not released yet)
    val avgGames1Col: Column = dfAvgGames1("avg_playtime")
    val avgGames2Col: Column = dfAvgGames2("avg_playtime")
    val avgDeltaCol: Column = abs(avgGames2Col.minus(avgGames1Col)).as("avg_playtime_delta")

    val dfAllAvgs: DataFrame = dfAvgGames2.join(dfAvgGames1, dfAvgGames2("appid") === dfAvgGames1("appid"), "left")

    val avgReleasedBefore: Double = dfAllAvgs
      .filter(dfAvgGames1("appid").isNotNull)
      .agg(avg(avgDeltaCol).as("avg_before"))
      .head()
      .getAs[Double]("avg_before")

    val avgReleasedAfter: Double = dfAllAvgs
      .filter(dfAvgGames1("appid").isNull)
      .agg(avg(avgGames2Col).as("avg_after"))
      .head()
      .getAs[Double]("avg_after")

    (avgReleasedBefore, avgReleasedAfter)
  }

  // Helper function to parse the timestamp format used in the trip dataset.
  private def parseTimestamp(timestamp: String) = LocalDateTime.from(timestampFormat.parse(timestamp))
}
