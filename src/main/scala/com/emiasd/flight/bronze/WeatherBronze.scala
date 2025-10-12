// com/emiasd/flight/bronze/WeatherBronze.scala
package com.emiasd.flight.bronze


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.emiasd.flight.io.Readers


object WeatherBronze {
  def readAndEnrich(spark: SparkSession, inputs: Seq[String]): DataFrame = {
    val raw = Readers.readTxt(spark, inputs, sep = ",", header = true, infer = true)
    val df = raw
      .withColumn("WBAN", upper(trim(coalesce(col("WBAN"), col("Wban")))))
      .withColumn("Date", regexp_replace(col("Date"), "[/]", "")) // 20120101
      .withColumn("Time", lpad(regexp_replace(col("Time"), ":", ""), 4, "0")) // 0000..2359
      .withColumn("obs_local_naive",
        to_timestamp(concat_ws(" ", col("Date"), col("Time")), "yyyyMMdd HHmm"))
      .withColumn("year", year(col("obs_local_naive")))
      .withColumn("month", format_string("%02d", month(col("obs_local_naive"))))
    df
  }
}