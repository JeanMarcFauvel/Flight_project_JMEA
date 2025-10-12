// com/emiasd/flight/silver/WeatherSlim.scala
package com.emiasd.flight.silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.emiasd.flight.util.DFUtils._
import com.emiasd.flight.io.Readers

object WeatherSlim {

  def enrichWithUTC(spark: SparkSession,
                    weatherBronze: DataFrame,
                    wbanTzPath: String): DataFrame = {

    // Lecture de la table de correspondance
    val tz = Readers.readCsv(spark, Seq(wbanTzPath))
      .select(
        col("WBAN").cast("string").as("WBAN"),
        col("AirportID").cast("int").as("airport_id"),
        col("TimeZone").cast("int").as("tz_hour")
      )
      .dropDuplicates("WBAN")

    // Jointure sur WBAN
    val joined = weatherBronze.join(tz, Seq("WBAN"), "left")

    // Conversion obs_local_naive → UTC
    val df = joined
      .withColumn("tz_offset_min", coalesce(col("tz_hour") * lit(60), lit(0)))
      .withColumn("obs_utc",addMinutes(col("obs_local_naive"), -col("tz_offset_min")))
      .withColumn("year", year(col("obs_utc")))
      .withColumn("month", format_string("%02d", month(col("obs_utc"))))

      // Sélection des colonnes utiles
      .select(
        col("airport_id"),
        col("WBAN"),
        col("obs_utc"),
        col("SkyCondition"),
        col("Visibility").cast("double"),
        col("DryBulbFarenheit").cast("double"),
        col("RelativeHumidity").cast("double"),
        col("WindSpeed").cast("double"),
        col("Altimeter").cast("double"),
        col("year"), col("month")
      )

    df
  }
}
