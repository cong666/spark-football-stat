package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class RunTasks extends FunSuite with BeforeAndAfterEach {
  val bucket = "/tmp/wiki-data"

  override protected def beforeEach(): Unit = {
    SparkSession.builder()
      .config("spark.ui.showConsoleProgress", "false")
      .master("local[*]")
      .getOrCreate()
  }

  override protected def afterEach(): Unit = {
    SparkSession.getActiveSession.foreach(_.close())
  }

  test("Run Tasks...") {
    Seq(
      Q1_WikiDocumentsToParquetTask(bucket),
      Q2_ShowLeagueStatsTask(bucket)
    ).foreach(_.run())
  }
}
