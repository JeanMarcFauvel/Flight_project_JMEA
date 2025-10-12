name         := "Flight_project_JMEA"
version      := "0.1.0"
scalaVersion := "2.12.18"

lazy val sparkVer  = "3.5.1"
lazy val deltaVer  = "3.2.0"   // compatible Spark 3.5.x

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVer % Provided,
    "org.apache.spark" %% "spark-mllib" % sparkVer % Provided,

    // Delta Lake (à embarquer côté appli car non fourni nativement)
    "io.delta" %% "delta-spark" % deltaVer,

    // Chargement de application.conf
    "com.typesafe" % "config" % "1.4.3",

    // Logging simple en local (facultatif ; ne pas mettre en Provided)
    "org.slf4j" % "slf4j-simple" % "2.0.13"
  )


// Options compilateur adaptées à Java 8
javacOptions  ++= Seq("-source", "1.8", "-target", "1.8")
scalacOptions ++= Seq("-deprecation", "-feature", "-Xlint")

// Conseillé pour exécuter Spark depuis IntelliJ
Compile / run / fork := true
Compile / run / javaOptions ++= Seq(
  "-Xms2g", "-Xmx6g",
  "-Duser.timezone=UTC"    // pour travailler proprement en UTC
)
