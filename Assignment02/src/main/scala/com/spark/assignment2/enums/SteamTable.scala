package com.spark.assignment2.enums

object SteamTable extends Enumeration {
  val AppInfo, Games1, Games2, PlayerSummaries = Value

  /**
    * Extension functions on this enum
    *
    * Based off this stackoverflow answer:
    *    https://stackoverflow.com/a/4348550
    * @param steamTable
    */
  class SteamTableExtensions(steamTable: Value) {

    /**
      * Gets the relative file path to the CSV file(s) containing
      * the table data.
      * @return
      */
    def CSVFilePath: String = steamTable match {
      case SteamTable.AppInfo         => "data/csv/app_id_info_r3pg.csv"
      case SteamTable.Games1          => "data/csv/games_1_r3pg.*.csv"
      case SteamTable.Games2          => "data/csv/games_2_r3pg.*.csv"
      case SteamTable.PlayerSummaries => "data/csv/player_summaries_r3pg.*.csv"
    }

    /**
      * Gets the relative file path to the Parquet directory that contains
      * the table information.
      * @return
      */
    def ParquetFilePath: String = steamTable match {
      case SteamTable.AppInfo         => "data/parquet/app_id_info_r3pg"
      case SteamTable.Games1          => "data/parquet/games_1_r3pg"
      case SteamTable.Games2          => "data/parquet/games_2_r3pg"
      case SteamTable.PlayerSummaries => "data/parquet/player_summaries_r3pg"
    }

    /**
      * Gets the Sql Server table name containing the table data.
      * @return
      */
    def SqlTableName: String = steamTable match {
      case SteamTable.AppInfo         => "app_id_info_r3pg"
      case SteamTable.Games1          => "games_1_r3pg"
      case SteamTable.Games2          => "games_2_r3pg"
      case SteamTable.PlayerSummaries => "player_summaries_r3pg"
    }

    /**
      * Expected amount of rows in the table
      * @return
      */
    def ExpectedRows: Int = steamTable match {
      case SteamTable.AppInfo         => 3428
      case SteamTable.Games1          => 11991574
      case SteamTable.Games2          => 19336223
      case SteamTable.PlayerSummaries => 656288
    }

    /**
      * Expected amount of columns in the table
      * @return
      */
    def ExpectedColumns: Int = steamTable match {
      case SteamTable.AppInfo         => 8
      case SteamTable.Games1          => 5
      case SteamTable.Games2          => 5
      case SteamTable.PlayerSummaries => 22
    }
  }
  implicit def value2SteamTable(steamTable: Value) = new SteamTableExtensions(steamTable)
}
