// com/emiasd/flight/join/BuildJT.scala
package com.emiasd.flight.join


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object BuildJT {
  private def selectWxCols(prefix: String) = Seq(
    col("obs_utc").as(s"${prefix}_ts"),
    col("SkyCondition").as(s"${prefix}_sky"),
    col("Visibility").cast("double").as(s"${prefix}_vis"),
    col("DryBulbFarenheit").cast("double").as(s"${prefix}_tempF"),
    col("RelativeHumidity").cast("double").as(s"${prefix}_rh"),
    col("WindSpeed").cast("double").as(s"${prefix}_wind"),
    col("Altimeter").cast("double").as(s"${prefix}_altim")
  )


  def buildJT(spark: SparkSession, flightsEnriched: DataFrame, weatherSlim: DataFrame, thMinutes: Int): DataFrame = {
    import spark.implicits._


    // wxOrigin
    val wxOrigin = weatherSlim.select(
      col("airport_id").as("o_airport_id"),
      col("obs_utc"),
      col("SkyCondition"),
      col("Visibility"),
      col("DryBulbFarenheit"),
      col("RelativeHumidity"),
      col("WindSpeed"),
      col("Altimeter")
    )

    // wxDest
    val wxDest = weatherSlim.select(
      col("airport_id").as("d_airport_id"),
      col("obs_utc"),
      col("SkyCondition"),
      col("Visibility"),
      col("DryBulbFarenheit"),
      col("RelativeHumidity"),
      col("WindSpeed"),
      col("Altimeter")
    )

    val f = flightsEnriched
      .filter(col("dep_ts_utc").isNotNull && col("arr_ts_utc").isNotNull)
      .withColumn("dep_minus_12h", col("dep_ts_utc") - expr("INTERVAL 12 HOURS"))
      .withColumn("arr_minus_12h", col("arr_ts_utc") - expr("INTERVAL 12 HOURS"))


    // Join météo origine (0..12h avant dep)
    val jO = f.join(wxOrigin, f("origin_airport_id") === wxOrigin("o_airport_id") &&
        wxOrigin("obs_utc").between(col("dep_minus_12h"), col("dep_ts_utc")), "left")
      .select(f.columns.map(col) ++ selectWxCols("o"): _*)


    // --- origine ---
    val aggO = jO
      .groupBy(f.columns.map(col): _*)
      .agg(
        sort_array(
          collect_list(
            struct(
              col("o_ts"),
              col("o_sky"),
              col("o_vis"),
              col("o_tempF"),
              col("o_rh"),
              col("o_wind"),
              col("o_altim")
            )
          ),
          asc = true
        ).as("Wo")
      )


    // Join météo destination (0..12h avant arr)
    val jD = aggO.join(wxDest, aggO("dest_airport_id") === wxDest("d_airport_id") &&
        wxDest("obs_utc").between(col("arr_minus_12h"), col("arr_ts_utc")), "left")
      .select(aggO.columns.map(col) ++ selectWxCols("d"): _*)


    // --- destination ---
    val aggD = jD
      .groupBy(aggO.columns.map(col): _*)
      .agg(
        sort_array(
          collect_list(
            struct(
              col("d_ts"),
              col("d_sky"),
              col("d_vis"),
              col("d_tempF"),
              col("d_rh"),
              col("d_wind"),
              col("d_altim")
            )
          ),
          asc = true
        ).as("Wd")
      )


    val withC = aggD.withColumn("C", when(col("ARR_DELAY_NEW") >= thMinutes, lit(1)).otherwise(lit(0)))


    // F struct minimal (tu pourras enrichir)
    val withF = withC.select(
      struct(
        col("UNIQUE_CARRIER").as("carrier"),
        col("FL_NUM").as("flnum"),
        col("FL_DATE").as("date"),
        col("ORIGIN").as("origin"),
        col("DEST").as("dest"),
        col("origin_airport_id"),
        col("dest_airport_id"),
        // (optionnel) pour garder l’heure planifiée brute:
        col("CRS_DEP_TIME").as("crs_dep_scheduled_hhmm"),
        // les références de temps utilisées pour les joins / features:
        col("dep_ts_utc"),
        col("arr_ts_utc"),
        col("ARR_DELAY_NEW").as("arr_delay_new"),
        col("flight_key")
      ).as("F"),
      col("Wo"), col("Wd"), col("C"), col("year"), col("month")
    )



    withF
  }
}