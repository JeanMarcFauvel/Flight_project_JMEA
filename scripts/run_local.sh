#!/usr/bin/env bash
set -euo pipefail

: "${SPARK_HOME:="$HOME/tools/spark-3.5.1"}"
if [ ! -x "$SPARK_HOME/bin/spark-submit" ]; then
  echo "❌ spark-submit introuvable sous SPARK_HOME=$SPARK_HOME"; exit 1
fi

# Build (JAR mince)
sbt package

# Tous les packages nécessaires au runtime (Delta + Config + SLF4J)
PKGS="io.delta:delta-spark_2.12:3.2.0,com.typesafe:config:1.4.3"

"$SPARK_HOME/bin/spark-submit" \
  --class com.emiasd.flight.Main \
  --master local[*] \
  --packages "$PKGS" \
  --conf spark.driver.memory=8g \
  --conf spark.executor.memory=4g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=8 \
  --conf spark.sql.files.maxPartitionBytes=268435456 \
  --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS \
  target/scala-2.12/flight_project_jmea_2.12-0.1.0.jar


