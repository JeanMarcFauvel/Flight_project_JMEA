// com/emiasd/flight/silver/WeatherUtcFilter.scala
package com.emiasd.flight.silver


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object WeatherUtcFilter {
  def toUtcAndFilter(weatherBronze: DataFrame, weatherPlan: DataFrame, mappingWban: DataFrame, airports: DataFrame): DataFrame = {
    val wWithAirport = weatherBronze
      .join(mappingWban, Seq("WBAN"), "left")
      .filter(col("airport_id").isNotNull)
      .withColumn("obs_utc", to_utc_timestamp(col("obs_local_naive"), col("timezone")))


    val airportSet = airports.select(col("airport_id").as("aoi")).distinct()


    val filtered = wWithAirport.join(airportSet, wWithAirport("airport_id") === airportSet("aoi"), "inner")
      .drop("aoi")


    filtered
      .withColumn("year", year(col("obs_utc")))
      .withColumn("month", format_string("%02d", month(col("obs_utc"))))
      .select(
        col("airport_id"), col("timezone"), col("obs_local_naive"), col("obs_utc"),
        col("WBAN"), col("StationName"), col("SkyCondition"), col("Visibility"),
        col("DryBulbFarenheit"), col("DewPointFarenheit"), col("RelativeHumidity"),
        col("WindSpeed"), col("Altimeter"),
        col("year"), col("month")
      )
  }
}