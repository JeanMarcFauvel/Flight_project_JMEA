// Databricks notebook source
// MAGIC %md
// MAGIC #Projet Flight - EMIASD promo 6
// MAGIC
// MAGIC ###Aurélien Duvignac-Rosa, Jean-Marc Fauvel, Edoardo Piciucchi

// COMMAND ----------

// MAGIC %md
// MAGIC ###Présentation
// MAGIC
// MAGIC Le projet Flight concerne le transport aérien intérieur US et a pour objectif de catégoriser les vols en deux groupes : ceux qui n'auront pas de retard dû à la météo, et ceux qui auront un retard supérieur à un seuil paramétrable dû à la météo.
// MAGIC
// MAGIC Pour réaliser cette catégorisation nous disposons comme base d'apprentissage de deux ensembles de fichiers :
// MAGIC
// MAGIC - 36 fichiers .csv mensuels traçant tous les vols intérieurs US entre le 01/01/2012 et le 31/12/2014
// MAGIC - 8 fichiers .txt mensuels traçant les données météorologiques de tous les capteurs météo US
// MAGIC
// MAGIC A ces fichiers s'ajoute un fichier .csv qui identifie la station météo de chaque aéroport.

// COMMAND ----------

// MAGIC %md
// MAGIC ##Initialisation de l'environnement
// MAGIC
// MAGIC Initialisation du SparkContext en configuration hadoop.
// MAGIC Vérification de la présence des dossiers flights et weather ainsi que du fichier wban_airport_timezone.csv
// MAGIC
// MAGIC Les fichiers ont été chargés dans Databricks via le menu File/Create table. Pour regrouper les fichiers par fonction, j'ai ajouté un dossier flights pour rassembler les 36 fichiers de vols et weather pour les 8 fichiers météo.

// COMMAND ----------

// Import utile
import org.apache.hadoop.fs._
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

// Lister un dossier
val path = new Path("/FileStore/tables")
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

fs.listStatus(path).foreach(x => println(x.getPath))


// COMMAND ----------

// MAGIC %md
// MAGIC Vérification de l'exhaustivité des chargements des fichiers météorologiques et du schéma.

// COMMAND ----------

println("Contenu de /FileStore/tables/weather/")
dbutils.fs.ls("/FileStore/tables/weather").foreach(f => println(f.name))

// COMMAND ----------

// MAGIC %md
// MAGIC Nous avons 8 fichiers météo chargés à l'adresse /FileStore/tables/weather.

// COMMAND ----------

// MAGIC %md
// MAGIC Vérification de l'exhaustivité des chargements des fichiers de vols et du schéma.

// COMMAND ----------

println("Contenu de /FileStore/tables/flights/")
dbutils.fs.ls("/FileStore/tables/flights").foreach(f => println(f.name))


// COMMAND ----------

// MAGIC %md
// MAGIC Nous avons bien chargé 36 fichiers mensuels d'activité des aéroports vols intérieurs US de 2012 à 2014.

// COMMAND ----------

// MAGIC %md
// MAGIC ##Lecture des fichiers

// COMMAND ----------

// MAGIC %md
// MAGIC Lecture des fichiers vols

// COMMAND ----------

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

// 1. Charger les fichiers de vols en RDD
def loadFlightRDDs(paths: Seq[String], spark: SparkSession): RDD[Array[String]] = {
  val sc = spark.sparkContext
  val base = sc.textFile(paths.head)
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(_._1.split(",", -1))

  paths.tail.foldLeft(base) { (rdd, path) =>
    val next = sc.textFile(path)
      .zipWithIndex()
      .filter(_._2 > 0)
      .map(_._1.split(",", -1))
    rdd.union(next)
  }
}

// 2. Charger les fichiers météo en RDD
def loadWeatherRDDs(paths: Seq[String], spark: SparkSession): RDD[Array[String]] = {
  val sc = spark.sparkContext
  val base = sc.textFile(paths.head)
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(_._1.split(",", -1))

  paths.tail.foldLeft(base) { (rdd, path) =>
    val next = sc.textFile(path)
      .zipWithIndex()
      .filter(_._2 > 0)
      .map(_._1.split(",", -1))
    rdd.union(next)
  }
}

// 3. Charger le mapping AirportId ↔ WBAN (à partir d’un CSV)
def loadWbanMapping(path: String, spark: SparkSession): Map[String, String] = {
  val sc = spark.sparkContext
  val lines = sc.textFile(path)
  val header = lines.first()
  lines.filter(_ != header)
    .map(_.split(",", -1))
    .filter(_.length >= 2)
    .map(arr => (arr(0).trim, arr(1).trim))
    .collect()
    .toMap
}

// 4. Extraire les AirportId ORIGIN/DEST des vols
def extractAirportIds(flightRDD: RDD[Array[String]], originIdx: Int, destIdx: Int): Set[String] = {
  flightRDD.flatMap(arr => Seq(arr(originIdx), arr(destIdx)))
    .filter(_.nonEmpty)
    .distinct()
    .collect()
    .toSet
}

// 5. Filtrer les lignes météo selon WBAN valides et ajouter l'AirportId correspondant
def filterWeatherByWban(
  weatherRDD: RDD[Array[String]],
  validWbans: Set[String],
  wbanIdx: Int,
  wbanToAirportId: Map[String, String]
): RDD[Array[String]] = {
  weatherRDD
    .filter(row => row.length > wbanIdx && validWbans.contains(row(wbanIdx).trim))
    .map(row => {
      val wban = row(wbanIdx).trim
      val airportId = wbanToAirportId.find(_._2 == wban).map(_._1).getOrElse("")
      row :+ airportId
    })
}

def printSample(rdd: org.apache.spark.rdd.RDD[Array[String]], n: Int = 5): Unit = {
  println(s"🧾 Aperçu des $n premières lignes :")
  rdd.take(n).zipWithIndex.foreach {
    case (arr, idx) =>
      println(f"[$idx%2d] " + arr.mkString(" | "))
  }
}


// COMMAND ----------

// Initialisation
val spark = SparkSession.builder().getOrCreate()

// 1. Paramétrage des mois de données
val months_f = Seq("201201", "201207", "201304")
val months_w = Seq("201201", "201207", "201304")

// 2. Construction des chemins vers les fichiers
val flightPaths = months_f.map(m => s"/FileStore/tables/flights/${m}.csv")
val weatherPaths = months_w.map(m => s"/FileStore/tables/weather/${m}hourly.txt")

// 3. Chargement en RDD
val flightsRDD = loadFlightRDDs(flightPaths, spark)
val weatherRDD = loadWeatherRDDs(weatherPaths, spark)


// COMMAND ----------

printSample(flightsRDD, 10)


// COMMAND ----------

printSample(weatherRDD, 5)

// COMMAND ----------

// MAGIC %md
// MAGIC Afficher le schéma des fichier flight

// COMMAND ----------

// MAGIC %md
// MAGIC Lecture des fichiers météo

// COMMAND ----------

// MAGIC %md
// MAGIC Afficher le schéma des fichiers météo

// COMMAND ----------

// MAGIC %md
// MAGIC ###Analyse des fichiers météo

// COMMAND ----------

// MAGIC %md
// MAGIC ###Création de la table des correspondances airportId / wban filtrées sur les aéroports origine et destination du flightsRDD
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC Dans le cadre de la recherche à limiter le volume de données au strict nécessaire à l'apprentissge, nous allons évaluer l'impact de réduire les données météos aux capteurs correspondants aux aéroports d'origine et de destination.
// MAGIC
// MAGIC Le fichier wban_airport_timezone.csv nous donne une correspondance entre les identifiants de balises météo et les Id des aéroports.

// COMMAND ----------

// MAGIC %md
// MAGIC Lecture de la table initiale de correspondances entre les aéroports et les balises météo

// COMMAND ----------

val wbanMap: Map[String, String] = loadWbanMapping(
  "/FileStore/tables/wban_airport_timezone.csv", spark
)

// Exemple : afficher quelques paires AirportId → WBAN
wbanMap.take(5).foreach { case (airportId, wban) =>
  println(s"AirportId: $airportId → WBAN: $wban")
}


// COMMAND ----------

// MAGIC %md
// MAGIC Extraction des airportId Origine et Destination contenus dans flightsRDD

// COMMAND ----------

val originIdx = 3  // exemple : ORIGIN_AIRPORT_ID
val destIdx   = 4  // exemple : DEST_AIRPORT_ID
val airportIds: Set[String] = extractAirportIds(flightsRDD, originIdx, destIdx)

println(s"✈️ Nombre d'aéroports O/D détectés : ${airportIds.size}")


// COMMAND ----------

// MAGIC %md
// MAGIC Filtre des lignes de weatherRDD pour ne conserver que celle correspondant aux balises des aéroports listés précédemment. Création d'une table de correspondance filtrée sur les aéroports de flightsRDD : validWbans, puis appel de la fonction filterWeatherByWban avec cette table filtrée en paramètre.

// COMMAND ----------

// 1. Mapping AirportId → WBAN (déjà chargé)
val wbanMap: Map[String, String] = loadWbanMapping("/FileStore/tables/wban_airport_timezone.csv", spark)

// 2. Inversion du mapping pour obtenir WBAN → AirportId
val wbanToAirportId: Map[String, String] = wbanMap.map(_.swap)

// 3. AirportIds présents dans les vols (déjà extraits)
val airportIds: Set[String] = extractAirportIds(flightsRDD, originIdx = 3, destIdx = 4) // adapt index si besoin

// 4. WBANs valides associés à ces AirportIds
val validWbans: Set[String] = airportIds.flatMap(wbanMap.get)

// 5. Index de la colonne WBAN dans weatherRDD
val wbanIdx = 0  // à ajuster selon le header météo

// 6. Appel de la fonction complète
val filteredWeatherRDD = filterWeatherByWban(weatherRDD, validWbans, wbanIdx, wbanToAirportId)


// COMMAND ----------

printSample(filteredWeatherRDD)

// COMMAND ----------

// MAGIC %md
// MAGIC Statistique de contrôle pour évaluer la différence entre le RDD météo d'origine et le RDD météo filtré.

// COMMAND ----------

val totalBefore  = weatherRDD.count()
val totalAfter   = filteredWeatherRDD.count()
val reductionPct = 100.0 * (totalBefore - totalAfter) / totalBefore

println(s"🌦 Lignes météo totales : $totalBefore")
println(s"🌦 Lignes après filtre WBAN : $totalAfter")
println(f"📉 Réduction : $reductionPct%.2f%%")


// COMMAND ----------

// MAGIC %md
// MAGIC ###Analyse
// MAGIC
// MAGIC En filtrant un fichier weather sur les aéroports on réduit sa taille de 94%.

// COMMAND ----------

// MAGIC %md
// MAGIC #Jointure des fichiers Flight et Weather

// COMMAND ----------

// MAGIC %md
// MAGIC # 📄 Pipeline optimisé de jointure Vols / Météo
// MAGIC
// MAGIC ## 🎯 Objectif
// MAGIC Ce pipeline permet de construire un RDD final `JT_RDD` contenant, pour chaque vol :
// MAGIC
// MAGIC - **F** → Informations complètes sur le vol (`Flight`)
// MAGIC - **Wo** → Observations météo à l’aéroport d’origine sur les 12h avant le départ
// MAGIC - **Wd** → Observations météo à l’aéroport de destination sur les 12h avant l’arrivée
// MAGIC - **C** → Classe : `1` si vol retardé (≥ seuil, ici 15 min), `0` sinon
// MAGIC
// MAGIC Il est structuré en **deux étapes successives** :  
// MAGIC 1. Jointure des vols avec la météo **à l’aéroport d’origine**  
// MAGIC 2. Jointure du résultat précédent avec la météo **à l’aéroport de destination**
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ## 📊 Schéma du pipeline
// MAGIC
// MAGIC ![Pipeline jointure vols/météo](https://raw.githubusercontent.com/JeanMarcFauvel/Flight_project_EMIASD6/main/Documentation/pipeline_jointure_colore_legende.png)
// MAGIC
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ## 🔍 Description des étapes
// MAGIC
// MAGIC ### **Phase 0 : Chargement et parsing**
// MAGIC 1. **Chargement brut**
// MAGIC    - `flightsRDD` : chargement CSV → `RDD[Array[String]]`
// MAGIC    - `filteredWeatherRDD` : chargement TXT météo filtré par WBAN → `RDD[Array[String]]`
// MAGIC 2. **Parsing**
// MAGIC    - `parseFlightArray` → transforme `Array[String]` en `Flight`
// MAGIC    - `parseWeatherArray` → transforme `Array[String]` en `WeatherObservation`
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### **Phase 1 : Jointure origine**
// MAGIC 1. **Tagging**
// MAGIC    - Vols (`Flight`) → `("FT", flight: Any)`
// MAGIC    - Météo (`WeatherObservation`) → `("OT", obs: Any)`
// MAGIC 2. **Union**
// MAGIC    - Unionne en `(String, Any)`
// MAGIC 3. **mapJoinFunction (origine)**
// MAGIC    - Construit une *composite key* `(airportId, date, tag)`
// MAGIC    - Duplique les FT si `tsd + 12h` tombe le jour suivant
// MAGIC 4. **Partitionnement**
// MAGIC    - `JoinKeyPartitioner` : partitionne sur `(airportId, date)` uniquement
// MAGIC    - Trie OT avant FT dans chaque partition
// MAGIC 5. **mapPartitions**
// MAGIC    - Construit AO (liste météo) pour chaque clé `(airportId, date)`
// MAGIC    - Associe à chaque FT la liste Wo (12h avant `tsd`)
// MAGIC 6. **Sortie**
// MAGIC    - `originJoinRDD` : `(Flight, Wo)`
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ### **Phase 2 : Jointure destination**
// MAGIC 1. **Tagging**
// MAGIC    - Résultat `(Flight, Wo)` → `("FT", (flight, wo): Any)`
// MAGIC    - Météo (`WeatherObservation`) → `("OT", obs: Any)`
// MAGIC 2. **Union**
// MAGIC    - Unionne en `(String, Any)`
// MAGIC 3. **mapJoinFunction (destination)**
// MAGIC    - Clé basée sur `(DEST_AIRPORT_ID, tsa)`
// MAGIC    - Duplique si `tsa + 12h` tombe le jour suivant
// MAGIC 4. **Partitionnement**
// MAGIC    - Même `JoinKeyPartitioner`
// MAGIC    - Tri OT avant FT
// MAGIC 5. **mapPartitions**
// MAGIC    - Construit AO (météo à destination) pour chaque clé `(airportId, date)`
// MAGIC    - Associe à chaque `(Flight, Wo)` la liste Wd (12h avant `tsa`)
// MAGIC    - Calcule `C` : 1 = retard, 0 = à l’heure
// MAGIC 6. **Sortie**
// MAGIC    - `JT_RDD` : `(Flight, Wo, Wd, C)`
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ## 📦 Structures de données
// MAGIC
// MAGIC | Variable           | Type                                                            | Description |
// MAGIC |--------------------|----------------------------------------------------------------|-------------|
// MAGIC | `Flight`           | case class                                                     | Infos vol complètes (Ao, Ad, horaires, retards, etc.) |
// MAGIC | `WeatherObservation` | case class                                                  | Infos météo complètes (airportId, datetime, T°, vent, humidité, etc.) |
// MAGIC | `Wo`               | `Seq[WeatherObservation]`                                      | Observations météo à l’origine (12h avant départ) |
// MAGIC | `Wd`               | `Seq[WeatherObservation]`                                      | Observations météo à destination (12h avant arrivée) |
// MAGIC | `C`                | `Int`                                                          | Classe : 1 = retard, 0 = à l’heure |
// MAGIC | `JT_RDD`           | `RDD[(Flight, Seq[WeatherObservation], Seq[WeatherObservation], Int)]` | RDD final pour le ML |
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC ## ⚡ Points clés d’optimisation
// MAGIC - **Partitionnement personnalisé** sur `(airportId, date)` → garantit que toutes les données d’une clé sont traitées ensemble
// MAGIC - **Tri secondaire** OT avant FT → permet de charger d’abord la météo (OT) puis d’associer aux vols (FT)
// MAGIC - **mapPartitions en flux** → pas de `toList` ou `groupBy`, évite de charger toute la partition en mémoire
// MAGIC - **Type Any** dans l’union** → compatibilité de type entre vols et météo tout en permettant de caster ensuite
// MAGIC
// MAGIC ---
// MAGIC
// MAGIC

// COMMAND ----------

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import java.time.{LocalDate, LocalDateTime}
import scala.collection.mutable.ListBuffer

// --------------------
// Case class complètes
// --------------------
case class Flight(
  FL_DATE: LocalDate,
  OP_CARRIER_AIRLINE_ID: Option[Int],
  OP_CARRIER_FL_NUM: Option[Int],
  ORIGIN_AIRPORT_ID: String, // Ao
  DEST_AIRPORT_ID: String,   // Ad
  CRS_DEP_TIME: Option[Int],
  ARR_DELAY_NEW: Option[Double],
  CANCELLED: Option[Double],
  DIVERTED: Option[Double],
  CRS_ELAPSED_TIME: Option[Double],
  WEATHER_DELAY: Option[Double],
  NAS_DELAY: Option[Double]
) {
  lazy val tsd: LocalDateTime = {
    val hour = CRS_DEP_TIME.getOrElse(0) / 100
    val minute = CRS_DEP_TIME.getOrElse(0) % 100
    FL_DATE.atTime(hour, minute)
  }
}

case class WeatherObservation(
  airportId: String, // A
  datetime: LocalDateTime, // t
  visibility: Option[Double],
  temperature: Option[Double],
  windSpeed: Option[Double],
  windDirection: Option[Double],
  humidity: Option[Double],
  pressure: Option[Double],
  weatherType: Option[String],
  skyCondition: Option[String]
)

// --------------------
// Helpers de parsing
// --------------------
implicit class StringOpt(s: String) {
  def toIntOption: Option[Int] = try { Option(s).filter(_.nonEmpty).map(_.toInt) } catch { case _: Throwable => None }
  def toDoubleOption: Option[Double] = try { Option(s).filter(_.nonEmpty).map(_.toDouble) } catch { case _: Throwable => None }
}

def parseFlightArray(arr: Array[String]): Flight = {
  Flight(
    FL_DATE = LocalDate.parse(arr(0)), // FL_DATE
    OP_CARRIER_AIRLINE_ID = arr(1).toIntOption,
    OP_CARRIER_FL_NUM = arr(2).toIntOption,
    ORIGIN_AIRPORT_ID = arr(3),
    DEST_AIRPORT_ID = arr(4),
    CRS_DEP_TIME = arr(5).toIntOption,
    ARR_DELAY_NEW = arr(6).toDoubleOption,
    CANCELLED = arr(7).toDoubleOption,
    DIVERTED = arr(8).toDoubleOption,
    CRS_ELAPSED_TIME = arr(9).toDoubleOption,
    WEATHER_DELAY = arr(10).toDoubleOption,
    NAS_DELAY = arr(11).toDoubleOption
  )
}

def parseWeatherArray(arr: Array[String]): WeatherObservation = {
  val dateStr = arr(1) // Date format yyyyMMdd
  val timeStr = arr(2) // Time format HHmm
  val datetime = LocalDateTime.parse(
    f"${dateStr.take(4)}-${dateStr.slice(4,6)}-${dateStr.takeRight(2)}T${timeStr.reverse.padTo(4,'0').reverse.grouped(2).mkString(":")}"
  )

  WeatherObservation(
    airportId = arr.last,
    datetime = datetime,
    visibility = arr(6).toDoubleOption,
    temperature = arr(12).toDoubleOption,
    windSpeed = arr(24).toDoubleOption,
    windDirection = arr(26).toDoubleOption,
    humidity = arr(22).toDoubleOption,
    pressure = arr(30).toDoubleOption,
    weatherType = Option(arr(8)).filter(_.nonEmpty),
    skyCondition = Option(arr(4)).filter(_.nonEmpty)
  )
}

// --------------------
// Partitioner
// --------------------
class JoinKeyPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions
  def getPartition(key: Any): Int = {
    val (airport, date, _) = key.asInstanceOf[(String, String, String)]
    (airport + date).hashCode % partitions
  }
}

// --------------------
// Fonction utilitaire
// --------------------
def getHourlyObservations(aObs: ListBuffer[WeatherObservation], baseTime: LocalDateTime): Seq[WeatherObservation] = {
  (0 to 12).flatMap { h =>
    val targetTime = baseTime.minusHours(h)
    aObs.find(o => o.datetime.getHour == targetTime.getHour && o.datetime.toLocalDate == targetTime.toLocalDate)
  }
}

val numPartitions = 200

// --------------------
// Étape 0 : Parsing vers RDD typés
// --------------------
val flightsTypedRDD: RDD[Flight] = flightsRDD.map(parseFlightArray)
val weatherTypedRDD: RDD[WeatherObservation] = filteredWeatherRDD.map(parseWeatherArray)

// --------------------
// Étape 1 : Join météo origine
// --------------------
val taggedOT_origin: RDD[(String, Any)] = weatherTypedRDD.map(obs => ("OT", obs: Any))
val taggedFT_origin: RDD[(String, Any)] = flightsTypedRDD.map(flight => ("FT", flight: Any))

val mappedRDD_origin = taggedOT_origin.union(taggedFT_origin)
  .flatMap {
    case ("OT", obs: WeatherObservation) =>
      val joinKey = (obs.airportId, obs.datetime.toLocalDate.toString)
      Seq(((joinKey._1, joinKey._2, "OT"), ("OT", obs)))

    case ("FT", flight: Flight) =>
      val joinKey1 = (flight.ORIGIN_AIRPORT_ID, flight.tsd.toLocalDate.toString)
      val compositeKey1 = (joinKey1._1, joinKey1._2, "FT")
      val emitList = ListBuffer((compositeKey1, ("FT", flight)))

      val plus12h = flight.tsd.plusHours(12)
      val plus1d = flight.tsd.plusDays(1)
      if (plus12h.toLocalDate == plus1d.toLocalDate) {
        val joinKey2 = (flight.ORIGIN_AIRPORT_ID, plus1d.toLocalDate.toString)
        val compositeKey2 = (joinKey2._1, joinKey2._2, "FT")
        emitList += ((compositeKey2, ("FT", flight)))
      }
      emitList
  }
  .repartitionAndSortWithinPartitions(new JoinKeyPartitioner(numPartitions))

val originJoinRDD = mappedRDD_origin.mapPartitions { iter =>
  val AO = ListBuffer[WeatherObservation]()
  val output = ListBuffer[(Flight, Seq[WeatherObservation])]()
  var currentKey: (String, String) = null

  iter.foreach {
    case ((airport, date, tag), ("OT", obs: WeatherObservation)) =>
      if (currentKey == null || currentKey != (airport, date)) {
        AO.clear()
        currentKey = (airport, date)
      }
      AO += obs

    case ((airport, date, tag), ("FT", flight: Flight)) =>
      if (currentKey == null || currentKey != (airport, date)) {
        AO.clear()
        currentKey = (airport, date)
      }
      val AT = getHourlyObservations(AO, flight.tsd)
      output += ((flight, AT))
  }
  output.iterator
}

// --------------------
// Étape 2 : Join météo destination
// --------------------
val taggedOT_dest: RDD[(String, Any)] = weatherTypedRDD.map(obs => ("OT", obs: Any))
val taggedFT_dest: RDD[(String, Any)] = originJoinRDD.map { case (flight, wo) => ("FT", (flight, wo): Any) }

val mappedRDD_dest = taggedOT_dest.union(taggedFT_dest)
  .flatMap {
    case ("OT", obs: WeatherObservation) =>
      val joinKey = (obs.airportId, obs.datetime.toLocalDate.toString)
      Seq(((joinKey._1, joinKey._2, "OT"), ("OT", obs)))

    case ("FT", (flight: Flight, wo: Seq[WeatherObservation])) =>
      val baseTime = flight.tsd.plusMinutes(flight.CRS_ELAPSED_TIME.getOrElse(0.0).toLong)
      val joinKey1 = (flight.DEST_AIRPORT_ID, baseTime.toLocalDate.toString)
      val compositeKey1 = (joinKey1._1, joinKey1._2, "FT")
      val emitList = ListBuffer((compositeKey1, ("FT", (flight, wo))))

      val plus12h = baseTime.plusHours(12)
      val plus1d = baseTime.plusDays(1)
      if (plus12h.toLocalDate == plus1d.toLocalDate) {
        val joinKey2 = (flight.DEST_AIRPORT_ID, plus1d.toLocalDate.toString)
        val compositeKey2 = (joinKey2._1, joinKey2._2, "FT")
        emitList += ((compositeKey2, ("FT", (flight, wo))))
      }
      emitList
  }
  .repartitionAndSortWithinPartitions(new JoinKeyPartitioner(numPartitions))

val JT_RDD: RDD[(Flight, Seq[WeatherObservation], Seq[WeatherObservation], Int)] =
  mappedRDD_dest.mapPartitions { iter =>
    val AO = ListBuffer[WeatherObservation]()
    val output = ListBuffer[(Flight, Seq[WeatherObservation], Seq[WeatherObservation], Int)]()
    var currentKey: (String, String) = null

    iter.foreach {
      case ((airport, date, tag), ("OT", obs: WeatherObservation)) =>
        if (currentKey == null || currentKey != (airport, date)) {
          AO.clear()
          currentKey = (airport, date)
        }
        AO += obs

      case ((airport, date, tag), ("FT", (flight: Flight, wo: Seq[WeatherObservation]))) =>
        if (currentKey == null || currentKey != (airport, date)) {
          AO.clear()
          currentKey = (airport, date)
        }
        val baseTime = flight.tsd.plusMinutes(flight.CRS_ELAPSED_TIME.getOrElse(0.0).toLong)
        val Wd = getHourlyObservations(AO, baseTime)
        val C = if (flight.ARR_DELAY_NEW.getOrElse(0.0) >= 15.0) 1 else 0
        output += ((flight, wo, Wd, C))
    }
    output.iterator
  }
