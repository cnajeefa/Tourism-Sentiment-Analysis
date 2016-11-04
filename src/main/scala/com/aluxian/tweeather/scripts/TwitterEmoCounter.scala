package com.aluxian.tweeather.scripts

import org.apache.spark.Logging

/**
  * This script is used to count the number of rows that [[TwitterEmoCollector]] has collected.
  */
object TwitterEmoCounter extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("tw/sentiment/emo/*.gz")

    // Print count
    logInfo(s"Count = ${data.filter(!_.startsWith("collected")).filter(!_.startsWith("Collected")).count()}")
    sc.stop()
  }

}
