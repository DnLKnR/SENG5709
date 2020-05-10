package com.spark.assignment2.models

import java.sql.Timestamp

/**
  * App Id Info data object
  * @param appid
  * @param Title
  * @param Type
  * @param Price
  * @param Release_Date
  * @param Rating
  * @param Required_Age
  * @param Is_Multiplayer
  */
case class AppInfo(
    appid: Int,
    Title: String,
    Type: String,
    Price: Double,
    Release_Date: Timestamp,
    Rating: Int,
    Required_Age: Int,
    Is_Multiplayer: Int
)
