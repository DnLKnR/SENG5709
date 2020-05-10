package com.spark.drivers

import com.spark.assignment2.enums.{SteamTable}
import com.spark.assignment2.models.{AppInfo, GameData, Player}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

abstract class DataDriver {
  /**
    * Encoders to assist converting a csv records into Case Classes.
    * They are 'implicit', meaning they will be picked up by implicit arguments,
    * which are hidden from view but automatically applied.
    */
  implicit val gameEncoder: Encoder[GameData] = Encoders.product[GameData]
  implicit val appInfoEncoder: Encoder[AppInfo] = Encoders.product[AppInfo]
  implicit val playerEncoder: Encoder[Player] = Encoders.product[Player]

  def readTableData(sparkSession: SparkSession, steamTable: SteamTable.Value): DataFrame

  /**
    * Gets app information based on the given App ID
    * @param spark
    * @param appId
    * @return
    */
  def getAppInfo(spark: SparkSession, appId: Int): AppInfo = {
    readTableData(spark, SteamTable.AppInfo)
      .toDF()
      .filter(col("appid").equalTo(lit(appId)))
      .as[AppInfo](appInfoEncoder)
      .first()
  }

  /**
    * Gets a player from the datasets based on the provided Steam ID
    * @param spark
    * @param steamId
    * @return
    */
  def getPlayer(spark: SparkSession, steamId: Long): Player = {
    readTableData(spark, SteamTable.PlayerSummaries)
      .toDF()
      .filter(col("steamid").equalTo(lit(steamId)))
      .as[Player](playerEncoder)
      .first()
  }
}
