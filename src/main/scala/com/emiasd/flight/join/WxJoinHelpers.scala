package com.emiasd.flight.join

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import com.emiasd.flight.util.DFUtils._

object WxJoinHelpers {

  /** Construit une structure météo robuste à partir d’un DataFrame `df` de weather.
   * Les noms de colonnes standard sont testés ; les manquants sont remplacés par null.
   */
  def buildWxStruct(df: DataFrame, prefix: String): Column = {
    struct(
      safeCol(df, "obs_utc", lit(null)).as(s"${prefix}_ts"),
      safeCol(df, "SkyCondition", lit(null)).as(s"${prefix}_sky"),
      safeCol(df, "Visibility", lit(null)).cast("double").as(s"${prefix}_vis"),
      safeCol(df, "DryBulbFarenheit", lit(null)).cast("double").as(s"${prefix}_tempF"),
      safeCol(df, "RelativeHumidity", lit(null)).cast("double").as(s"${prefix}_rh"),
      safeCol(df, "WindSpeed", lit(null)).cast("double").as(s"${prefix}_wind"),
      safeCol(df, "Altimeter", lit(null)).cast("double").as(s"${prefix}_altim")
    )
  }

  /** Teste la présence d'une colonne dans un DataFrame. */
  def has(df: DataFrame, colName: String): Boolean = df.columns.contains(colName)
}
