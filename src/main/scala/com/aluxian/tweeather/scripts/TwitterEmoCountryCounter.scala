package com.aluxian.tweeather.scripts

import org.apache.spark.Logging

/**
  * Created by Najeefa Nikhat Choudhury
  * This script is used to count the number of rows that [[TwitterEmoCountryCollector]] has collected.
  */
object TwitterEmoCountryCounter extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Import data
    logInfo("Parsing text files")
    val data = sc.textFile("tw/sentiment/emoByCountry/*.gz")

    // Print count
    logInfo(s"Count = ${data.filter(!_.startsWith("collected")).filter(!_.startsWith("Collected")).count()}")
    sc.stop()
  }

}
