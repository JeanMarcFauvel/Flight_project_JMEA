name         := "Flight_project_JMEA"
version      := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql"   % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided
)

// Options compilateur adaptées à Java 8
javacOptions  ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint")
