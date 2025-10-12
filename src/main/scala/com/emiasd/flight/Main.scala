package com.emiasd.flight

import com.emiasd.flight.config.AppConfig
import com.emiasd.flight.spark.{SparkBuilder, PathResolver}
import com.emiasd.flight.io.{Readers, Writers}
import com.emiasd.flight.bronze.{FlightsBronze, WeatherBronze}
import com.emiasd.flight.silver.{CleaningPlans, MappingWBAN, WeatherSlim}
import com.emiasd.flight.join._


object Main {
  def main(args: Array[String]): Unit = {
    val cfg  = AppConfig.load()                  // lit application.conf
    val spark = SparkBuilder.build(cfg)
    import spark.implicits._

    val paths = PathResolver.resolve(cfg)

    // === BRONZE ===
    val flightsBronze = FlightsBronze.readAndEnrich(
      spark, paths.flightsInputs, paths.mapping)
    // Vérifie l’unicité des colonnes
    val dupCols = flightsBronze.columns.groupBy(_.toLowerCase).collect { case (n, arr) if arr.length > 1 => n }
    require(dupCols.isEmpty, s"Duplicate columns: ${dupCols.mkString(", ")}")

    val weatherBronze = WeatherBronze.readAndEnrich(
      spark, paths.weatherInputs)

    Writers.writeDelta(flightsBronze.coalesce(2), paths.bronzeFlights, Seq("year","month"), overwriteSchema = true)
    Writers.writeDelta(weatherBronze.coalesce(2), paths.bronzeWeather, Seq("year","month"), overwriteSchema = true)

    // === SILVER ===
    val flightsPlan = CleaningPlans.deriveFlightsPlan(flightsBronze)
    val flightsSilver = CleaningPlans.cleanFlights(flightsBronze, flightsPlan)
    Writers.writeDelta(flightsSilver.coalesce(2), paths.silverFlights, Seq("year","month"), overwriteSchema = true)

    // Météo → UTC par offset fixe d'heures (pas de DST)
    val weatherSlim = WeatherSlim.enrichWithUTC(spark, weatherBronze, paths.mapping)
    Writers.writeDelta(weatherSlim.coalesce(2), paths.silverWeatherFiltered, Seq("year","month"), overwriteSchema = true)

    // === JOIN → JT ===
    val flightsPrepared = Readers.readDelta(spark, paths.silverFlights)
    val weatherSlimDF     = Readers.readDelta(spark, paths.silverWeatherFiltered)
    val flightsEnriched = FlightsEnriched.build(flightsPrepared, weatherSlimDF)
    val jt              = BuildJT.buildJT(spark, flightsEnriched, weatherSlimDF, cfg.thMinutes)

    Writers.writeDelta(jt, paths.goldJT, Seq("year","month"), overwriteSchema = true)

    // Contrôles rapides
    println(s"JT écrit → ${paths.goldJT}")
    println("Lignes JT: " + Readers.readDelta(spark, paths.goldJT).count())

    spark.stop()
  }
}
