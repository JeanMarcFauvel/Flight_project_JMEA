package com.emiasd.flight

import com.emiasd.flight.config.AppConfig
import com.emiasd.flight.spark.{SparkBuilder, PathResolver}
import com.emiasd.flight.io.{Readers, Writers}
import com.emiasd.flight.bronze.{FlightsBronze, WeatherBronze}
import com.emiasd.flight.silver.{CleaningPlans, MappingWBAN, WeatherSlim}
import com.emiasd.flight.join._
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val cfg  = AppConfig.load()                  // lit application.conf
    val spark = SparkBuilder.build(cfg)
    spark.sparkContext.setLogLevel("WARN")        // "ERROR" si on le veut encore plus silencieux
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
    val flightsEnriched = FlightsEnriched.build(flightsPrepared)
    val jtOut              = BuildJT.buildJT(spark, flightsEnriched, weatherSlimDF, cfg.thMinutes)

    Writers.writeDelta(jtOut, paths.goldJT, Seq("year","month"), overwriteSchema = true)

    // Contrôles rapides
    println(s"JT écrit → ${paths.goldJT}")
    println("Lignes JT: " + Readers.readDelta(spark, paths.goldJT).count())

    // === Vérifications qualité ===


    val jtCheck = Readers.readDelta(spark, paths.goldJT)

    // 1️⃣ Nombre de lignes
    println("JT rows      = " + jtCheck.count)

    // 2️⃣ Unicité de la clé vol
    println("JT distinct flight_key = " + jtCheck.select($"F.flight_key").distinct.count)

    // 3️⃣ Présence des timestamps (alternative robuste sur champs imbriqués)
    jtCheck.agg(
      sum(when(col("F.dep_ts_utc").isNull, 1).otherwise(0)).as("null_dep"),
      sum(when(col("F.arr_ts_utc").isNull, 1).otherwise(0)).as("null_arr"),
      count(lit(1)).as("total")
    ).show(false)

    jtCheck.printSchema()

    // 4️⃣ Part des vols avec observations météo Wo / Wd
    val withFlags = jtCheck.withColumn("hasWo", size($"Wo") > 0).withColumn("hasWd", size($"Wd") > 0)
    withFlags.agg(
      avg(when($"hasWo", lit(1)).otherwise(lit(0))).as("pct_with_Wo"),
      avg(when($"hasWd", lit(1)).otherwise(lit(0))).as("pct_with_Wd")
    ).show(false)

    // 5️⃣ Aperçu visuel (top 10)
    jtCheck.select($"F.carrier", $"F.flnum", $"F.date", $"F.origin_airport_id", $"F.dest_airport_id",
        $"C", size($"Wo").as("nWo"), size($"Wd").as("nWd"))
      .orderBy(desc("nWo"))
      .show(10,false)

    // === Fin contrôles ===
    spark.stop()
  }
}
