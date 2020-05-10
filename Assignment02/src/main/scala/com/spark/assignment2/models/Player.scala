package com.spark.assignment2.models

import java.sql.Timestamp

/**
  * Player Summary data object
  * @param steamid
  * @param personaname
  * @param profileurl
  * @param avatar
  * @param avatarmedium
  * @param avatarfull
  * @param personastate
  * @param communityvisibilitystate
  * @param profilestate
  * @param lastlogoff
  * @param commentpermission
  * @param realname
  * @param primaryclanid
  * @param timecreated
  * @param gameid
  * @param gameserverip
  * @param gameextrainfo
  * @param cityid
  * @param loccountrycode
  * @param locstatecode
  * @param loccityid
  * @param dateretrieved
  */
case class Player(
    steamid: Long,
    personaname: String,
    profileurl: String,
    avatar: String,
    avatarmedium: String,
    avatarfull: String,
    personastate: String,
    communityvisibilitystate: Int,
    profilestate: String,
    lastlogoff: Timestamp,
    commentpermission: String,
    realname: String,
    primaryclanid: String,
    timecreated: Timestamp,
    gameid: String,
    gameserverip: String,
    gameextrainfo: String,
    cityid: String,
    loccountrycode: String,
    locstatecode: String,
    loccityid: String,
    dateretrieved: Timestamp
)
