// com/emiasd/flight/io/Writers.scala
package com.emiasd.flight.io


import org.apache.spark.sql.{DataFrame, SaveMode}


object Writers {
  def writeDelta(df: DataFrame, path: String, partitions: Seq[String] = Nil, overwriteSchema: Boolean = false): Unit = {
    val w = df.write.format("delta")
      .mode(SaveMode.Overwrite)
      .option("overwriteSchema", overwriteSchema.toString)
    val finalW = if (partitions.nonEmpty) w.partitionBy(partitions:_*) else w
    finalW.save(path)
  }
}