// com/emiasd/flight/join/FlightsEnriched.scala
package com.emiasd.flight.join

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object FlightsEnriched {
  /** Les timestamps UTC sont déjà calculés en Bronze.
   * Ici on ajoute uniquement une clé de vol stable.
   */
  def build(flights: DataFrame, weatherSlim: DataFrame): DataFrame = {
   flights.withColumn(
        "flight_key",
        concat_ws("-", col("UNIQUE_CARRIER"), col("FL_NUM").cast("string"), col("FL_DATE"), col("ORIGIN"), col("DEST"))
   )
  }
}