package com.aluxian.tweeather.scripts

import com.aluxian.tweeather.streaming.TwitterUtils
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import twitter4j.FilterQuery
import java.util.Locale

/**
  * This script uses Twitter Streaming API to collect tweets along with country code. It uses the Twitter app, whose credentials
  * are stored '''com/aluxian/tweeather/res/twitter.properties'''.
  */
object TwitterEmoCountryCollector extends Script with Logging {

  override def main(args: Array[String]) {
    super.main(args)

    // Initialize a map to convert Countries from 2 chars iso encoding to 3 characters
    val iso2toIso3Map: Map[String, String] = Locale.getISOCountries()
      .map(iso2 => iso2 -> new Locale("", iso2).getISO3Country)
      .toMap


    val ssc = new StreamingContext(sc, streamingInterval)
    val stream = TwitterUtils.createMultiStream(ssc, queryBuilder)

    stream
      .filter(status=> status.getPlace != null)
      .map(status => {
      val text = status.getText.replaceAll("[\\n\\r]+", " ")
      val countryCode = iso2toIso3Map.getOrElse(status.getPlace.getCountryCode, "")
      Seq(countryCode, status.getCreatedAt.getTime, text).mkString("||")
      })
      .repartition(sc.defaultParallelism)
      .saveAsTextFiles("tw/sentiment/emoByCountry/collected/", "text")

    ssc.start()

    if (!ssc.awaitTerminationOrTimeout(streamingTimeout)) {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }

  def queryBuilder(): FilterQuery = {
    new FilterQuery()
      .track(keyWords:_*)
      .language("en")
  }

val keyWords = TwitterEmoCollector.keyWords

//mapping chart: Countries from 2 chars iso encoding to 3 characters
/* CV -> CPV, MA -> MAR, AO -> AGO, VN -> VNM,
 IN -> IND, KW -> KWT, ML -> MLI, ID -> IDN, JE -> JEY, HM -> HMD, EG -> EGY,
 BG -> BGR, SG -> SGP, SV -> SLV, TC -> TCA, TH -> THA, AT -> AUT, GQ -> GNQ, BD -> BGD,
 TR -> TUR, HT -> HTI, UM -> UMI, MY -> MYS, RU -> RUS, MH -> MHL, NI -> NIC, BZ -> BLZ,
 KP -> PRK, VE -> VEN, IL -> ISR, GD -> GRD, GI -> GIB, TN -> TUN, DM -> DMA, PR -> PRI,
 NF -> NFK, MO -> MAC, TW -> TWN, KN -> KNA, PH -> PHL, WF -> WLF, JO -> JOR, ME -> MNE,
 ES -> ESP, AZ -> AZE, MR -> MRT, SM -> SMR, BL -> BLM, PK -> PAK, NZ -> NZL, GP -> GLP,
 NA -> NAM, JM -> JAM, GU -> GUM, SB -> SLB, AX -> ALA, CM -> CMR, US -> USA, MV -> MDV,
 SI -> SVN, CW -> CUW, BH -> BHR, AN -> ANT, VG -> VGB, HK -> HKG, SD -> SDN, AD -> AND,
 RO -> ROU, LU -> LUX, VC -> VCT, FO -> FRO, GL -> GRL, BW -> BWA, CF -> CAF, CI -> CIV,
 BV -> BVT, KY -> CYM, KG -> KGZ, LY -> LBY, MM -> MMR, MZ -> MOZ, SZ -> SWZ, IE -> IRL,
 IR -> IRN, EH -> ESH, IQ -> IRQ, BB -> BRB, FK -> FLK, NP -> NPL, BE -> BEL, AU -> AUS,
 TZ -> TZA, UY -> URY, SA -> SAU, ZW -> ZWE, MD -> MDA, PG -> PNG, AF -> AFG, MU -> MUS,
 SL -> SLE, HU -> HUN, GT -> GTM, BO -> BOL, TM -> TKM, NE -> NER, CL -> CHL, FI -> FIN,
 MN -> MNG, NO -> NOR, GG -> GGY, EE -> EST, KM -> COM, LT -> LTU, ER -> ERI, SH -> SHN,
 SY -> SYR, LC -> LCA, CC -> CCK, PL -> POL, CH -> CHE, ST -> STP, NG -> NGA, TF -> ATF,
 KI -> KIR, LV -> LVA, UG -> UGA, CY -> CYP, MF -> MAF, PM -> SPM, MW -> MWI, CG -> COG,
 IS -> ISL, BI -> BDI, TK -> TKL, SE -> SWE, AE -> ARE, KZ -> KAZ, LB -> LBN, AR -> ARG,
 GS -> SGS, BF -> BFA, DJ -> DJI, HR -> HRV, BS -> BHS, RS -> SRB, BA -> BIH, WS -> WSM,
 GB -> GBR, SJ -> SJM, FR -> FRA, GM -> GMB, LS -> LSO, UZ -> UZB, PF -> PYF, AG -> ATG,
 GW -> GNB, FJ -> FJI, CO -> COL, ZM -> ZMB, AQ -> ATA, GF -> GUF, SO -> SOM, MT -> MLT,
 NU -> NIU, BN -> BRN, RW -> RWA, PT -> PRT, PW -> PLW, KH -> KHM, SX -> SXM, TJ -> TJK,
 KR -> KOR, SS -> SSD, PY -> PRY, AM -> ARM, MC -> MCO, CX -> CXR, TT -> TTO, UA -> UKR,
 LI -> LIE, BR -> BRA, PA -> PAN, MQ -> MTQ, TG -> TGO, FM -> FSM, NR -> NRU, PN -> PCN,
 GN -> GIN, YT -> MYT, CD -> COD, GA -> GAB, MG -> MDG, AI -> AIA, YE -> YEM, HN -> HND,
 IT -> ITA, RE -> REU, DO -> DOM, IO -> IOT, CZ -> CZE, GH -> GHA, GR -> GRC, AS -> ASM,
 ZA -> ZAF, GY -> GUY, BY -> BLR, LK -> LKA, BT -> BTN, OM -> OMN, CK -> COK, KE -> KEN,
 MX -> MEX, SK -> SVK, MK -> MKD, DZ -> DZA, QA -> QAT, CU -> CUB, TL -> TLS, DK -> DNK,
 BJ -> BEN, VI -> VIR, NL -> NLD, LA -> LAO, CA -> CAN, BM -> BMU, JP -> JPN, AW -> ABW,
 TO -> TON, CN -> CHN, VU -> VUT, AL -> ALB, ET -> ETH, IM -> IMN, SN -> SEN, PE -> PER,
 BQ -> BES, CR -> CRI, VA -> VAT, NC -> NCL, MP -> MNP, GE -> GEO, TD -> TCD, SC -> SYC,
 PS -> PSE, EC -> ECU, TV -> TUV, LR -> LBR, MS -> MSR, DE -> DEU, SR -> SUR*/

}
