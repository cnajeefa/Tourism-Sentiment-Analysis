package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.streaming.TwitterUtils
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import twitter4j.FilterQuery

/**
  * This script uses Twitter Streaming API to collect tweets which contain one or more emoji
  * characters and are written in English. It uses one or more Twitter apps, whose credentials
  * are stored '''com/aluxian/tweeather/res/twitter.properties'''.
  */

  /**
  * Changes:
  * New emojis used for travel apart were added in this script
  * The tweets are collected using keywords and language filter instead of emojis
    */
object TwitterEmoCollector extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    val ssc = new StreamingContext(sc, streamingInterval)
    val stream = TwitterUtils.createMultiStream(ssc, queryBuilder)

    stream
      .map(_.getText.replaceAll("[\\n\\r]+", " "))
      .repartition(sc.defaultParallelism)
      .saveAsTextFiles("tw/sentiment/emo/collected/", "text")

    ssc.start()

    if (!ssc.awaitTerminationOrTimeout(streamingTimeout)) {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  def queryBuilder(): FilterQuery = {
    new FilterQuery()
      .track(keyWords: _*)
      .language("en")
  }


  //filter using popular keywords used for tourism and travel
  val keyWords = Seq(
  "tourism",
  "touristic spot",
  "travel",
  "trip",
  "holiday",
  "tour",
  "traveling",
  "vacation",
  "holiday view",
  "holiday destination",
  "visiting",
  "tourist",
  "traveler",
  "traveller",
  "travelblog",
  "sightseen",
  "sightseeing",
  "summerholiday",
  "winterholiday",
  "trekking",
  "hiking",
  "niceview",
  "travel photography",
  "ttot",
  "TravelTuesday",
  "TBEX",
  "MexMonday",
  "BeachThursday",
  "rtw",
  "travelmassive",
  "wanderlust"
  )


  val positiveEmoticons = Seq(
    "\uD83D\uDE0D", // SMILING FACE WITH HEART-SHAPED EYES
    "\uD83D\uDE0A", // SMILING FACE WITH SMILING EYES
    "\uD83D\uDE03", // SMILING FACE WITH OPEN MOUTH
    "\uD83D\uDE02", // FACE WITH TEARS OF JOY
    "\uD83D\uDE18", // FACE THROWING A KISS
    "\uD83D\uDE01", // GRINNING FACE WITH SMILING EYES
    "\uD83D\uDE1A", // KISSING FACE WITH CLOSED EYES
    "\uD83D\uDC95", // TWO HEARTS
    "\uD83D\uDC4C", // OK HAND SIGN
    "\uD83D\uDC4D", // THUMBS UP SIGN
    "\uD83D\uDE38", // GRINNING CAT FACE WITH SMILING EYES
    "\uD83D\uDE39", // CAT FACE WITH TEARS OF JOY
    "\uD83D\uDE3A", // SMILING CAT FACE WITH OPEN MOUTH
    "\uD83D\uDE3B", // SMILING CAT FACE WITH HEART-SHAPED EYES
    "\uD83D\uDE3D", // KISSING CAT FACE WITH CLOSED EYES
    "\uD83D\uDC93", // BEATING HEART
    "\uD83D\uDC96", // SPARKLING HEART
    "\uD83D\uDC97", // GROWING HEART
    "\uD83D\uDC99", // BLUE HEART
    "\uD83D\uDC9A", // GREEN HEART
    "\uD83D\uDC9B", // YELLOW HEART
    "\uD83D\uDC9C", // PURPLE HEART
    "\uD83D\uDC9D", // HEART WITH RIBBON
    "\uD83D\uDC9E", // REVOLVING HEARTS
    "\uD83D\uDC9F", // HEART DECORATION
    "\uD83C\uDF89", // PARTY POPPER
    "\uD83D\uDE0E", // SMILING FACE WITH SUNGLASSES
    "\uD83D\uDE00", // GRINNING FACE
    "\uD83D\uDE07", // SMILING FACE WITH HALO
    "\uD83D\uDE08", // SMILING FACE WITH HORNS
    "\uD83D\uDE17", // KISSING FACE
    "\uD83D\uDE19", // KISSING FACE WITH SMILING EYES
    "\uD83D\uDE0B", // FACE SAVOURING DELICIOUS FOOD
    "\uD83D\uDC8B", // KISS MARK
    "\u2665", // BLACK HEART SUIT
    "\u2764", // HEAVY BLACK HEART
    "\u263A", // WHITE SMILING FACE
    ":)",
    ":-)",
    "=)",
    ":D",
    // added additional emoticons used in Tourism/travel
    "\uD83D\uDE4C", //RAISED HANDS IN CELEBRATION
    "\uD83D\uDC4F", //CLAPPING HANDS
    "\uD83C\uDF34", //PALM TREE
    "\uD83C\uDF35", //CACTUS
    "\u2600", //SUN WITH RAYS
    "\uD83C\uDF1E", //SUN WITH FACE
    "\u26C5", //SUN BEHIND CLOUD (partially sunny day)
    "\uD83C\uDF08", //RAINBOW
    "\u26F1", //UMBRELLA ON GROUND
    "\u2744", //SNOWFLAKES
    "\u2603", //SNOWMAN
    "\u26C4", //SNOWMAN WITHOUT SNOW
    "\uD83C\uDF83", //JOCK-O-LANTERN
    "\uD83C\uDF86", //FIREWORKS
    "\u2728", //SPARKLES
    "\uD83C\uDFBF", //SKIS
    "\uD83C\uDF7A", //BEER MUG
    "\u2615", //HOT BEVERAGE
    "\uD83C\uDF7B", //CLINKING BEER MUGS
    "\uD83C\uDF77", //WINE GLASS
    "\uD83C\uDF78", //COCKTAIL GLASS
    "\uD83C\uDF79", //TROPICAL DRINK
    "\uD83C\uDF69", //DOUGNUT
    "\uD83D\uDC4D", //THUMBS UP
    "\uD83D\uDC4C", //OK HAND
    "\uD83C\uDF41", //MAPLE LEAF
    "\uD83C\uDF42", //FALLEN LEAF
    "\uD83D\uDE06", //SMILING FACE WITH OPEN MOUTH AND TIGHTLY-CLOSED EYES
    "\uD83D\uDE04", //SMILING FACE WITH OPEN MOUTH AND SMILING EYES
    "\uD83D\uDE05", //SMILING FACE WITH OPEN MOUTH AND COLD SWEAT
    "\uD83D\uDE09", //WINKING FACE
    "\uD83D\uDE0B", //FACE SAVOURING DELICIOUS FOOD
    "\uD83D\uDE0C", //RELIEVED FACE
    "\uD83D\uDE0F", //SMIRKING FACE
    "\uD83D\uDE1C", //FACE WITH STUCK-OUT TONGUE AND WINKING EYE
    "\uD83D\uDE1B" //FACE WITH STUCK-OUT TONGUE
  )

  val negativeEmoticons = Seq(
    "\uD83D\uDE12", // UNAMUSED FACE
    "\uD83D\uDE2D", // LOUDLY CRYING FACE
    "\uD83D\uDE29", // WEARY FACE
    "\uD83D\uDE14", // PENSIVE FACE
    "\uD83D\uDE33", // FLUSHED FACE
    "\uD83D\uDE48", // SEE-NO-EVIL MONKEY
    "\uD83D\uDE13", // FACE WITH COLD SWEAT
    "\uD83D\uDE16", // CONFOUNDED FACE
    "\uD83D\uDE1D", // FACE WITH STUCK-OUT TONGUE AND TIGHTLY-CLOSED EYES
    "\uD83D\uDE1E", // DISAPPOINTED FACE
    "\uD83D\uDE20", // ANGRY FACE
    "\uD83D\uDE21", // POUTING FACE
    "\uD83D\uDE22", // CRYING FACE
    "\uD83D\uDE23", // PERSEVERING FACE
    "\uD83D\uDE25", // DISAPPOINTED BUT RELIEVED FACE
    "\uD83D\uDE28", // FEARFUL FACE
    "\uD83D\uDE2A", // SLEEPY FACE
    "\uD83D\uDE2B", // TIRED FACE
    "\uD83D\uDE30", // FACE WITH OPEN MOUTH AND COLD SWEAT
    "\uD83D\uDE31", // FACE SCREAMING IN FEAR
    "\uD83D\uDE32", // ASTONISHED FACE
    "\uD83D\uDE35", // DIZZY FACE
    "\uD83D\uDE3E", // POUTING CAT FACE
    "\uD83D\uDE3F", // CRYING CAT FACE
    "\uD83D\uDE40", // WEARY CAT FACE
    "\uD83D\uDC4E", // THUMBS DOWN SIGN
    "\uD83D\uDC94", // BROKEN HEART
    "\uD83D\uDC7F", // ANGRY FACE WITH HORNS
    "\uD83D\uDCA9", // PILE OF POO
    "\uD83D\uDE15", // CONFUSED FACE
    "\uD83D\uDE1F", // WORRIED FACE
    "\uD83D\uDE27", // ANGUISHED FACE
    "\uD83D\uDE26", // FROWNING FACE WITH OPEN MOUTH
    "\uD83D\uDE2E", // FACE WITH OPEN MOUTH
    ":(",
    ":-(",
    // added additional emoticons used in Tourism/travel
    "\uD83D\uDC4E", // THUMBS DOWN
    "\u26C8", // CLOUD WITH LIGHTNING AND RAIN
    "\uD83C\uDF27", //CLOUD WITH RAIN
    "\uD83C\uDF28", //CLOUD WITH SNOW
    "\uD83C\uDF29", //CLOUD WITH LIGHTNING
    "\u2602", //UMBRELLA
    "\u2614", //UMBRELLA WITH RAIN DROPS
    "\uD83D\uDE2C" //GRIMACING FACE
  )

}
