#!/usr/bin/env bash
set -euo pipefail

: "${SPARK_HOME:="$HOME/tools/spark-3.5.1"}"

sbt package

PKGS="io.delta:delta-spark_2.12:3.2.0,com.typesafe:config:1.4.3"

"$SPARK_HOME/bin/spark-submit" \
  --class com.emiasd.flight.Main \
  --master yarn \
  --deploy-mode client \
  --packages "$PKGS" \
  --conf spark.app.name=Flight_Project_JMEA \
  --conf spark.executor.instances="${EXECUTORS:-4}" \
  --conf spark.executor.cores="${CORES:-2}" \
  --conf spark.executor.memory="${EXEC_MEM:-4g}" \
  --conf spark.driver.memory="${DRIVER_MEM:-3g}" \
  target/scala-2.12/flight_project_jmea_2.12-0.1.0.jar

