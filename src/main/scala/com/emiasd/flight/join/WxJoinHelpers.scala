// com/emiasd/flight/join/WxJoinHelpers.scala
package com.emiasd.flight.join

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import com.emiasd.flight.util.DFUtils._

object WxJoinHelpers {
  /** Structure météo robuste (null si absent), prête pour Wo/Wd */
  def buildWxStruct(df: DataFrame, prefix: String): Column = {
    struct(
      safeCol(df, "obs_utc",           lit(null)).as(s"${prefix}_ts"),
      safeCol(df, "SkyCondition",      lit(null)).as(s"${prefix}_sky"),
      safeCol(df, "WeatherType",       lit(null)).as(s"${prefix}_wxType"),
      safeCol(df, "Visibility",        lit(null)).cast("double").as(s"${prefix}_vis"),
      safeCol(df, "TempC",             lit(null)).cast("double").as(s"${prefix}_tempC"),
      safeCol(df, "DewPointC",         lit(null)).cast("double").as(s"${prefix}_dewC"),
      safeCol(df, "RelativeHumidity",  lit(null)).cast("double").as(s"${prefix}_rh"),
      safeCol(df, "WindSpeedKt",       lit(null)).cast("double").as(s"${prefix}_windKt"),
      safeCol(df, "WindDirection",     lit(null)).cast("double").as(s"${prefix}_windDir"),
      safeCol(df, "Altimeter",         lit(null)).cast("double").as(s"${prefix}_altim"),
      safeCol(df, "SeaLevelPressure",  lit(null)).cast("double").as(s"${prefix}_slp"),
      safeCol(df, "StationPressure",   lit(null)).cast("double").as(s"${prefix}_stnp"),
      safeCol(df, "HourlyPrecip",      lit(null)).cast("double").as(s"${prefix}_precip")
    )
  }

  /** Teste la présence d'une colonne dans un DataFrame. */
  def has(df: DataFrame, colName: String): Boolean = df.columns.contains(colName)
}
