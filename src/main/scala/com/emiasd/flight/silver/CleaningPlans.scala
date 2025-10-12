// com/emiasd/flight/silver/CleaningPlans.scala
package com.emiasd.flight.silver


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.emiasd.flight.util.DFUtils._  // pour safeCol


object CleaningPlans {
  def deriveFlightsPlan(df: DataFrame): DataFrame = df // hook pour logique future


  def cleanFlights(df: DataFrame, plan: DataFrame): DataFrame = {
    df
      .filter(coalesce(col("CANCELLED"), lit(0)) === 0 && coalesce(col("DIVERTED"), lit(0)) === 0)
      .select(
        // colonnes réellement présentes en bronze
        col("CRS_DEP_TIME"),
        col("ARR_DELAY_NEW"),
        col("origin_airport_id"), col("dest_airport_id"),
        // facultatives selon le CSV : on les prend si elles existent
        safeCol(df, "UNIQUE_CARRIER", lit(null).cast("string")).as("UNIQUE_CARRIER"),
        safeCol(df, "ORIGIN",          lit(null).cast("string")).as("ORIGIN"),
        safeCol(df, "DEST",            lit(null).cast("string")).as("DEST"),
        // time features issues du bronze
        col("FL_DATE"), col("FL_NUM"),
        col("dep_ts_utc"), col("arr_ts_utc"),
        col("year"), col("month")
      )
  }


  def deriveWeatherPlan(df: DataFrame, missingnessThreshold: Double): DataFrame = df // placeholder


  def airportsOfInterest(f: DataFrame): DataFrame = {
    f.select(col("origin_airport_id").as("airport_id")).union(
      f.select(col("dest_airport_id").as("airport_id"))
    ).distinct()
  }
}