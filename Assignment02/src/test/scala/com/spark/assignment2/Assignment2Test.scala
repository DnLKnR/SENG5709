package com.spark.assignment2

import org.scalatest.BeforeAndAfterEach
import scala.concurrent.duration._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  /**
   * Set this value to 'true' to halt after execution so you can view the Spark UI at localhost:4040.
   * NOTE: If you use this, you must terminate your test manually.
   * OTHER NOTE: You should only use this if you run a test individually.
   */
  val BLOCK_ON_COMPLETION = false;

  /**
   * Create a SparkSession that runs locally on our laptop.
   */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 2")
      .master("local[*]")
      .getOrCreate()

  /**
   * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
   * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
   */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

  /**
   * 'What game has the highest average play time?' Test Method
   */
  test("Highest Average Playtime - Single Game") {
    val (expectedAppId, expectedTitle, expectedAvg): (Int, String, Double) = (570, "Dota 2", 17680)
    val (actualAppId, actualTitle, actualAvg): (Int, String, Double) = Assignment2.problem1(spark)

    actualAppId must equal (expectedAppId)
    actualTitle must equal (expectedTitle)
    actualAvg must equal (expectedAvg +- 1)
  }

  /**
   * What game has the highest amount of users in the dataset? Test Method
   */
  test("Most Users - Single Game") {
    val expected: (Int, String, Long) = (10, "Counter-Strike", 655297)
    val actual: (Int, String, Long) = Assignment2.problem2(spark)
    actual must equal (expected)
  }

  /**
   * 'What user has the highest play time for a single game?' Test Method
   */
  test("Max Playtime for a Game - Single User") {
    val expected: (Int, String, Long, String, Int) =
      (240, "Counter-Strike: Source", 76561197961828654L, "[U][A]-VampeD", 2434347)
    val actual: (Int, String, Long, String, Int) = Assignment2.problem3(spark)
    actual must equal (expected)
  }

  /**
   * 'What user has the highest total play time?' Test Method
   */
  test("Max Total Playtime - Single User") {
    val expected: (Long, String, Long) = (76561197961828654L, "[U][A]-VampeD", 2461190)
    val actual: (Long, String, Long) = Assignment2.problem4(spark)
    actual must equal (expected)
  }

  /**
   * 'What game has the highest ratio of [number of hours played] to [average hour per user]?'
   * Test Method
   */
  test("Highest Ratio of [Total Hours Played]:[Average Hour Per User]") {
    val (expectedAppId, expectedTitle, expectedAvgPlaytime): (Int, String, Double) = (10, "Counter-Strike", 655297)
    val (actualAppId, actualTitle, actualAvgPlaytime): (Int, String, Double) = Assignment2.problem5(spark)
    actualAppId must equal (expectedAppId)
    actualTitle must equal (expectedTitle)
    actualAvgPlaytime must equal (expectedAvgPlaytime +- 1)
  }

  /**
   * 'What is the total amount of users per Required Age?' Test Method
   */
  test("Total Users per Game's Required Age") {
    val requiredAgeToUserCountExpected: Map[Int, Long] = Map(
      0 -> 15799912,
      6 -> 289,
      10 -> 2423,
      12 -> 2347,
      13 -> 70745,
      15 -> 10680,
      16 -> 125695,
      17 -> 3172141,
      18 -> 151991,
    )
    val dfUsersPerRequiredAge: DataFrame = Assignment2.problem6(spark)
    for ((requiredAge, expectedUserCount) <- requiredAgeToUserCountExpected) {
      val actualUserCount: Long = dfUsersPerRequiredAge
        .filter(col("Required_Age").equalTo(lit(requiredAge)))
        .first()
        .getAs[Long]("user_count")
      actualUserCount must equal (expectedUserCount)
    }
  }

  /**
   * 'What game had the biggest increase in average playtime from the earliest to latest retrieval
   * date?' Test Method
   */
  test("Single Game - Largest Average Playtime Increase") {
    val (expectedAppId, expectedTitle, expectedAvgDelta): (Int, String, Double) =
      (227400, "Darkfall Unholy Wars", 13541)
    val (actualAppId, actualTitle, actualAvgDelta): (Int, String, Double) = Assignment2.problem7(spark)

    actualAppId must equal (expectedAppId)
    actualTitle must equal (expectedTitle)
    actualAvgDelta must equal (expectedAvgDelta +- 1)
  }

  /**
   * 'Did games released during the data retrieval have a higher increase in average play time? Or did
   * pre-existing games have a higher increase in average play time?' Test Method
   */
  test("Total average playtime increase for games release before and after initial data collection") {
    val (beforeExpected, afterExpected): (Double, Double) = (254.323462d, 258.172802d)
    val (beforeActual, afterActual): (Double, Double) = Assignment2.problem8(spark)
    // less tolerance since the values are rather close
    beforeActual must equal (beforeExpected +- 0.5)
    afterActual must equal (afterExpected +- 0.5)
  }
}
