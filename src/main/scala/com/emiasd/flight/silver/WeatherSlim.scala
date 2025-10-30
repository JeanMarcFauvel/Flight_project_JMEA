// com/emiasd/flight/silver/WeatherSlim.scala
package com.emiasd.flight.silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.emiasd.flight.io.Readers
import com.emiasd.flight.util.DFUtils._

object WeatherSlim {

  def enrichWithUTC(spark: SparkSession,
                    weatherBronze: DataFrame,
                    wbanTzPath: String): DataFrame = {

    val tz = Readers.readCsv(spark, Seq(wbanTzPath))
      .select(
        col("WBAN").cast("string").as("WBAN"),
        col("AirportID").cast("int").as("airport_id"),
        col("TimeZone").cast("int").as("tz_hour")
      )
      .dropDuplicates("WBAN")

    val joined = weatherBronze.join(tz, Seq("WBAN"), "left")

    val df = joined
      .withColumn("tz_offset_min", coalesce(col("tz_hour") * lit(60), lit(0)))
      .withColumn("obs_utc", addMinutes(col("obs_local_naive"), -col("tz_offset_min")))
      // ==== Standardisation des unités ====
      // TempC (°C) prioritaire; sinon converti depuis °F
      .withColumn("TempC",
        coalesce(col("DryBulbCelsius"),
          (col("DryBulbFarenheit") - lit(32.0)) * lit(5.0/9.0)))
      .withColumn("DewPointC",
        coalesce(col("DewPointCelsius"),
          (col("DewPointFarenheit") - lit(32.0)) * lit(5.0/9.0)))
      // Vent : on garde la vitesse telle quelle (les QCLCD sont souvent en mph ou kt selon source).
      // Si tu sais l’unité exacte de ton CSV, convertis ici vers m/s ou kt.
      .withColumnRenamed("WindSpeed", "WindSpeedRaw")
      .withColumn("WindSpeedKt", col("WindSpeedRaw")) // TODO: convertir si nécessaire
      // Pression : on garde les 3 mesures disponibles (Altimeter inHg, SLP/StationPressure hPa)
      //
      .withColumn("year",  year(col("obs_utc")))
      .withColumn("month", date_format(col("obs_utc"), "MM"))
      .select(
        col("airport_id"), col("WBAN"),
        col("obs_utc"),
        col("SkyCondition"), col("WeatherType"),
        col("Visibility").cast("double"),
        col("TempC").cast("double"), col("DewPointC").cast("double"),
        col("RelativeHumidity").cast("double"),
        col("WindSpeedKt").cast("double"), col("WindDirection").cast("double"),
        col("Altimeter").cast("double"), col("SeaLevelPressure").cast("double"),
        col("StationPressure").cast("double"),
        col("HourlyPrecip").cast("double"),
        col("year"), col("month")
      )

    df
  }
}
