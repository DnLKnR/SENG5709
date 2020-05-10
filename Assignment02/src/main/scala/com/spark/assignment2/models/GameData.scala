package com.spark.assignment2.models

import java.sql.Timestamp

/**
  * Player's Game Information that was pulled starting August 14, 2014
  *
  * @param steamid
  * @param appid
  * @param playtime_2weeks
  * @param playtime_forever
  * @param dateretrieved
  */
case class GameData(
    steamid: Long,
    appid: Int,
    playtime_2weeks: Option[Int],
    playtime_forever: Option[Int],
    dateretrieved: Timestamp
)
