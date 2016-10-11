package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.RichBoolean
import org.apache.spark.Logging
import org.apache.spark.sql.SaveMode

/**
  * This script parses the tweets collected by [[TwitterEmoByCountry]].
  * It removes duplicates and tweets which contain both positive and negative emojis.
  * The resulting dataset is coalesced to reduce the number of partitions.
  */
object TwitterEmoCountryParser extends Script with Logging {

  val positiveEmoticons = TwitterEmoCollector.positiveEmoticons
  val negativeEmoticons = TwitterEmoCollector.negativeEmoticons

  override def main(args: Array[String]) {
    super.main(args)
    import sqlc.implicits._

    // Import data
    //for neutral sentiment do (hasPositive & hasNegative)
    logInfo("Parsing text files")
    val data = sc.textFile("tw/sentiment/emoByCountry/*.gz")
      .coalesce(sc.defaultParallelism)
      .map(_.stripPrefix("RT").trim)
      .distinct()
      .filter(!_.startsWith("Collected"))
      .map(_.split("\\|\\|"))
      .map(row => (row(0), parseLong(row(1)).getOrElse(0L), row(2)))
  /*    .map(text => {
        val hasPositive = positiveEmoticons.exists(text.contains)
        val hasNegative = negativeEmoticons.exists(text.contains)
        if (hasPositive ^ hasNegative) (text, hasPositive.toDouble) else null
      })
      .filter(_ != null)*/

    logInfo("Saving text files")
    data.toDF("country_code", "time_stamp", "tweet_text").write.mode(SaveMode.Overwrite)
      .parquet("tw/sentiment/emoByCountry/parsed/data.parquet")

    logInfo("Parsing finished")
    sc.stop()
  }

  def parseLong(str: String):Option[Long] = {
    try {
      Some(str.toLong)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def parseDouble(str: String):Option[Double] = {
    try {
      Some(str.toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

}