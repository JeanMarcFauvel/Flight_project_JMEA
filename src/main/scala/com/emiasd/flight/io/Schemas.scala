// com/emiasd/flight/io/Schemas.scala
package com.emiasd.flight.io


import org.apache.spark.sql.types._


object Schemas {
  // Possibles schémas BTS et NCDC (restons permissifs, on lit en inferSchema côté Readers)
  val flightsSelected: Seq[String] = Seq(
    "YEAR","MONTH","DAY_OF_MONTH","CRS_DEP_TIME","CRS_ARR_TIME",
    "ARR_DELAY_NEW","CANCELLED","DIVERTED",
    "ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID",
    "ORIGIN","DEST","FL_DATE","FL_NUM","UNIQUE_CARRIER"
  )


  val weatherKeep: Seq[String] = Seq(
    "WBAN","Date","Time","StationName","SkyCondition","Visibility","DryBulbFarenheit",
    "DewPointFarenheit","RelativeHumidity","WindSpeed","Altimeter"
  )
}