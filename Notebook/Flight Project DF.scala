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
import org.apache.spark.Partitioner
import java.time.{LocalDate, LocalDateTime}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession

// COMMAND ----------

// =======================================================
// Imports & confs
// =======================================================
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728) // ~128MB

// COMMAND ----------

// MAGIC %md
// MAGIC ##Vérifier que les fichiers ont bien été chargés dans l'environnement

// COMMAND ----------

// MAGIC %md
// MAGIC Lister les dossiers et fichiers existants dans /FileStore/tables

// COMMAND ----------

// Lister les dossiers stockés dans /tables
val path = new Path("/FileStore/tables")
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

fs.listStatus(path).foreach(x => println(x.getPath))


// COMMAND ----------

// MAGIC %md
// MAGIC Vérification de l'exhaustivité des chargements des fichiers météorologiques.

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

// =======================================================
// Schémas EXACTS d’après ton descriptif
// =======================================================

// flights.csv
val flightsSchema = StructType(Seq(
  StructField("FL_DATE", DateType,    true),
  StructField("OP_CARRIER_AIRLINE_ID", IntegerType, true),
  StructField("OP_CARRIER_FL_NUM",     IntegerType, true),
  StructField("ORIGIN_AIRPORT_ID",     IntegerType, true),
  StructField("DEST_AIRPORT_ID",       IntegerType, true),
  StructField("CRS_DEP_TIME",          IntegerType, true),
  StructField("ARR_DELAY_NEW",         DoubleType,  true),
  StructField("CANCELLED",             DoubleType,  true),
  StructField("DIVERTED",              DoubleType,  true),
  StructField("CRS_ELAPSED_TIME",      DoubleType,  true),
  StructField("WEATHER_DELAY",         DoubleType,  true),
  StructField("NAS_DELAY",             DoubleType,  true),
  StructField("_c12",                  StringType,  true)
))

// weather hourly.txt (⚠️ pas d'Airport_id dans le RAW)
val weatherSchema = StructType(Seq(
  StructField("WBAN",                      StringType, true),
  StructField("Date",                      StringType, true),
  StructField("Time",                      StringType, true),
  StructField("StationType",               StringType, true),
  StructField("SkyCondition",              StringType, true),
  StructField("SkyConditionFlag",          StringType, true),
  StructField("Visibility",                StringType, true),
  StructField("VisibilityFlag",            StringType, true),
  StructField("WeatherType",               StringType, true),
  StructField("WeatherTypeFlag",           StringType, true),
  StructField("DryBulbFarenheit",          StringType, true),
  StructField("DryBulbFarenheitFlag",      StringType, true),
  StructField("DryBulbCelsius",            StringType, true),
  StructField("DryBulbCelsiusFlag",        StringType, true),
  StructField("WetBulbFarenheit",          StringType, true),
  StructField("WetBulbFarenheitFlag",      StringType, true),
  StructField("WetBulbCelsius",            StringType, true),
  StructField("WetBulbCelsiusFlag",        StringType, true),
  StructField("DewPointFarenheit",         StringType, true),
  StructField("DewPointFarenheitFlag",     StringType, true),
  StructField("DewPointCelsius",           StringType, true),
  StructField("DewPointCelsiusFlag",       StringType, true),
  StructField("RelativeHumidity",          StringType, true),
  StructField("RelativeHumidityFlag",      StringType, true),
  StructField("WindSpeed",                 StringType, true),
  StructField("WindSpeedFlag",             StringType, true),
  StructField("WindDirection",             StringType, true),
  StructField("WindDirectionFlag",         StringType, true),
  StructField("ValueForWindCharacter",     StringType, true),
  StructField("ValueForWindCharacterFlag", StringType, true),
  StructField("StationPressure",           StringType, true),
  StructField("StationPressureFlag",       StringType, true),
  StructField("PressureTendency",          StringType, true),
  StructField("PressureTendencyFlag",      StringType, true),
  StructField("PressureChange",            StringType, true),
  StructField("PressureChangeFlag",        StringType, true),
  StructField("SeaLevelPressure",          StringType, true),
  StructField("SeaLevelPressureFlag",      StringType, true),
  StructField("RecordType",                StringType, true),
  StructField("RecordTypeFlag",            StringType, true),
  StructField("HourlyPrecip",              StringType, true),
  StructField("HourlyPrecipFlag",          StringType, true),
  StructField("Altimeter",                 StringType, true),
  StructField("AltimeterFlag",             StringType, true)
))

// =======================================================
// Helpers
// =======================================================

// Flights: construit les timestamps planifiés sans référence circulaire
def withScheduledTimestamps(df: DataFrame): DataFrame = {
  val hhmm  = lpad(col("CRS_DEP_TIME").cast("string"), 4, "0")
  val depTs = to_timestamp(
    concat(date_format(col("FL_DATE"), "yyyy-MM-dd"), lit(" "), hhmm),
    "yyyy-MM-dd HHmm"
  )
  val depEpoch = unix_timestamp(depTs)
  val arrSec   = (col("CRS_ELAPSED_TIME").cast(IntegerType) * 60).cast("long")
  val arrTs    = when(col("CRS_ELAPSED_TIME").isNotNull && depTs.isNotNull,
                      to_timestamp(from_unixtime(depEpoch + arrSec)))
                 .otherwise(lit(null).cast(TimestampType))

  df.withColumn("CRS_DEP_TS", depTs)
    .withColumn("CRS_ARR_TS", arrTs)
}

// Weather Bronze: parse Date/Time → timestamp local “naïf”
def withWeatherTimestampsNoTZ(df: DataFrame): DataFrame = {
  val dateDigits = regexp_replace(col("Date"), "[^0-9]", "")
  val dateParsed = coalesce(
    to_date(dateDigits, "yyyyMMdd"),
    to_date(col("Date"), "yyyy-MM-dd")
  )
  val timeDigits = regexp_replace(col("Time"), "[^0-9]", "")
  val timeHHmm   = lpad(timeDigits, 4, "0")
  val obsLocal   = to_timestamp(concat(date_format(dateParsed, "yyyy-MM-dd"), lit(" "), timeHHmm), "yyyy-MM-dd HHmm")
  df.withColumn("obs_local_naive", obsLocal)
}

def addYearMonth(df: DataFrame, tsCol: String): DataFrame =
  df.withColumn("year", year(col(tsCol))).withColumn("month", month(col(tsCol)))

// Active/désactive l’overwrite du schéma pendant le dev
val DEV = true

def writeDelta(df: DataFrame, path: String, parts: Seq[String], overwriteSchemaInDev: Boolean = DEV): Unit = {
  val base = df.write.format("delta")
    .mode("overwrite")                 // réécrit le contenu
    .partitionBy(parts: _*)

  val writer =
    if (overwriteSchemaInDev) base.option("overwriteSchema", "true") // remplace le schéma
    else base

  writer.save(path)
}

def peek(df: DataFrame, n: Int = 5): Unit = { df.show(n, truncate=false); df.printSchema() }

// Conversion per-row TZ → UTC (évite de passer un Column comme 2e arg. de to_utc_timestamp)
def withUtcFromPerRowTimezone(weatherWithMap: DataFrame): DataFrame = {
  val tzs: Array[String] =
    weatherWithMap.select(coalesce(col("tz"), lit("UTC")).as("tz")).distinct().collect().map(_.getString(0))

  val obsUtcCol: Column = tzs.foldLeft(lit(null).cast(TimestampType): Column) { (acc, tz) =>
    when(coalesce(col("tz"), lit("UTC")) === lit(tz),
         to_utc_timestamp(col("obs_local_naive"), tz)
    ).otherwise(acc)
  }

  weatherWithMap
    .withColumn("tz_effective", coalesce(col("tz"), lit("UTC")))
    .withColumn("obs_utc_ts", obsUtcCol)
    .withColumn("obs_hour_utc", date_trunc("hour", col("obs_utc_ts")))
    .withColumn("airport_id", col("airport_id").cast(IntegerType))
    .withColumn("year", year(col("obs_utc_ts")))
    .withColumn("month", month(col("obs_utc_ts")))
}

// COMMAND ----------

// =========================
// CONFIG
// =========================
val MISSINGNESS_THRESHOLD = 0.60  // 60% : ajuste selon besoin

val months_f = Seq("201201", "201207", "201304")
val months_w = Seq("201201", "201207", "201304")
val flightPaths  = months_f.map(m => s"/FileStore/tables/flights/${m}.csv")
val weatherPaths = months_w.map(m => s"/FileStore/tables/weather/${m}hourly.txt")
val mappingPath  = "/FileStore/tables/wban_airport_timezone.csv"

val baseDeltaBronze = "dbfs:/delta/bronze"
val baseDeltaSilver = "dbfs:/delta/silver"
val flightsBronzePath = s"$baseDeltaBronze/flights"
val weatherBronzePath = s"$baseDeltaBronze/weather"
val flightsSilverPath = s"$baseDeltaSilver/flights"
val weatherSilverFilteredPath = s"$baseDeltaSilver/weather_filtered"

// =========================
// HELPERS analyse
// =========================
def missingness(df: DataFrame): DataFrame = {
  val stringCols = df.schema.fields.collect { case StructField(n, StringType, _, _) => n }.toSet
  val total = df.count()
  val exprs = df.columns.map { c =>
    val isBlank = if (stringCols.contains(c)) length(trim(col(c))) === 0 else lit(false)
    sum( when(col(c).isNull.or(isBlank), 1).otherwise(0) ).cast("long").alias(c)
  }
  val wide = df.agg(exprs.head, exprs.tail:_*)
  val arr  = array(df.columns.map(c => struct(lit(c).as("column"), col(c).cast("long").as("nulls"))):_*)
  wide.select(explode(arr).as("kv"))
      .select(col("kv.column"), col("kv.nulls"))
      .withColumn("rows", lit(total))
      .withColumn("null_pct", round(col("nulls")/col("rows")*100, 2))
}

// =========================
// 1) BRONZE: Lecture + enrichissements minimaux
// =========================
val flightsBronze = spark.read
  .option("header","true").option("mode","PERMISSIVE")
  .option("dateFormat","yyyy-MM-dd")
  .schema(flightsSchema)
  .csv(flightPaths:_*)
  .drop("_c12")                                             // drop direct
  .transform(withScheduledTimestamps)                       // CRS_DEP_TS / CRS_ARR_TS
  .withColumnRenamed("ORIGIN_AIRPORT_ID","origin_airport_id")
  .withColumnRenamed("DEST_AIRPORT_ID",  "dest_airport_id")
  .transform(df => addYearMonth(df, "FL_DATE"))

val weatherBronze = spark.read
  .option("header","true").option("mode","PERMISSIVE")
  .schema(weatherSchema)
  .csv(weatherPaths:_*)
  .withColumn("wban", upper(trim(col("WBAN"))))
  .transform(withWeatherTimestampsNoTZ)                     // obs_local_naive (naïf)
  .transform(df => addYearMonth(df, "obs_local_naive"))

writeDelta(flightsBronze, flightsBronzePath, Seq("year","month"))
writeDelta(weatherBronze, weatherBronzePath, Seq("year","month"))






// COMMAND ----------

// =========================
// 2) ANALYSE → DÉCISIONS DE NETTOYAGE
// =========================

// Flights: plan de nettoyage
case class FlightsCleanPlan(dropCols: Seq[String], filterExpr: Column)
def deriveFlightsPlan(df: DataFrame): FlightsCleanPlan = {
  // règles issues de ton DQ : supprimer lignes annulées/diverties, puis colonnes CANCELLED/DIVERTED
  val filterExpr = coalesce(col("CANCELLED"), lit(0.0)) === 0.0 && coalesce(col("DIVERTED"), lit(0.0)) === 0.0
  val toDrop = df.columns.intersect(Array("CANCELLED","DIVERTED"))
  FlightsCleanPlan(toDrop, filterExpr)
}

// Weather: plan de nettoyage dynamique (colonnes très manquantes + flags)
case class WeatherCleanPlan(colsToDrop: Seq[String])
def deriveWeatherPlan(df: DataFrame, threshold: Double): WeatherCleanPlan = {
  val miss = missingness(df)
  val highMissCols = miss.filter(col("null_pct") >= lit(threshold*100))
                         .select("column").as[String].collect().toSeq
  val flagCols = df.columns.filter(_.endsWith("Flag"))
  // Protéger les colonnes-clefs utilisées plus tard
  val protectedCols = Set("WBAN","wban","Date","Time","obs_local_naive","year","month")
  val dropCols = (highMissCols ++ flagCols).distinct.filterNot(protectedCols)
  WeatherCleanPlan(dropCols)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##Phase silver

// COMMAND ----------

// =========================
// 3) SILVER: Nettoyage + mapping WBAN→(airport_id, tz) + UTC + filtre aéroports
// =========================

// flights silver
val flightsPlan   = deriveFlightsPlan(flightsBronze)
val flightsSilver = flightsBronze
  .filter(flightsPlan.filterExpr)
  .drop(flightsPlan.dropCols:_*)
  .transform(df => addYearMonth(df, "FL_DATE"))

writeDelta(flightsSilver, flightsSilverPath, Seq("year","month"))

// mapping WBAN → AirportId + Timezone (IANA)
val mappingDF = spark.read
  .option("header","true").option("mode","PERMISSIVE")
  .csv(mappingPath)
  .withColumn("wban", upper(trim(col("WBAN"))))
  .withColumn("airport_id", col("AirportId").cast(IntegerType))
  .withColumn("tz", coalesce(col("Timezone"), lit("UTC")))
  .select("wban","airport_id","tz")
  .dropDuplicates("wban")

// aéroports d’intérêt (depuis flights nettoyé)
val airportsOfInterest = flightsSilver
  .select(col("origin_airport_id").as("airport_id"))
  .union(flightsSilver.select(col("dest_airport_id").as("airport_id")))
  .filter(col("airport_id").isNotNull)
  .dropDuplicates()

// weather silver (nettoyage + enrichissement + UTC + filtre)
val weatherPlan = deriveWeatherPlan(weatherBronze, MISSINGNESS_THRESHOLD)

val weatherSilverFiltered = weatherBronze
  .drop(weatherPlan.colsToDrop:_*)
  .join(broadcast(mappingDF), Seq("wban"), "inner")       // ajoute airport_id, tz
  .transform(withUtcFromPerRowTimezone)                   // crée obs_utc_ts, obs_hour_utc (UTC)
  .join(airportsOfInterest, Seq("airport_id"), "inner")   // restreint aux aéroports utiles
  .transform(df => addYearMonth(df, "obs_utc_ts"))

writeDelta(weatherSilverFiltered, weatherSilverFilteredPath, Seq("year","month"))

// (option) OPTIMIZE/ZORDER si Delta Lake disponible
// spark.sql(s"OPTIMIZE delta.`$weatherSilverFilteredPath` ZORDER BY (airport_id, obs_hour_utc)")
// spark.sql(s"OPTIMIZE delta.`$flightsSilverPath`          ZORDER BY (origin_airport_id, dest_airport_id, FL_DATE)")

// COMMAND ----------

// (option) OPTIMIZE/ZORDER si Delta Lake disponible
spark.sql(s"OPTIMIZE delta.`$weatherSilverFilteredPath` ZORDER BY (airport_id, obs_hour_utc)")
spark.sql(s"OPTIMIZE delta.`$flightsSilverPath`          ZORDER BY (origin_airport_id, dest_airport_id, FL_DATE)")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Jointure en deux étapes
// MAGIC

// COMMAND ----------

// =======================
// 0) Imports & chemins
// =======================
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val baseDeltaSilver            = "dbfs:/delta/silver"
val weatherSilverFilteredPath  = s"$baseDeltaSilver/weather_filtered"

// ⚠️ Chemin de tes vols déjà nettoyés & préparés (CANCELLED/DIVERTED enlevés, colonnes propres,
// timestamps locaux construits: CRS_DEP_TS & CRS_ARR_TS)
val flightsPreparedPath        = s"$baseDeltaSilver/flights"   // <- adapte si besoin

val baseDeltaGold              = "dbfs:/delta/gold"
val thMinutes                  = 60                                  // seuil T_h (ex: 15, 30, 45, 60, 90)
val jtPath                     = s"$baseDeltaGold/JT_th$thMinutes"

// =======================
// 1) Lecture des sources
// =======================
val weatherSlim0 = spark.read.format("delta").load(weatherSilverFilteredPath)
val flightsSlim0 = spark.read.format("delta").load(flightsPreparedPath)

// Ajoute obs_hour_utc si absent (arrondi à l’heure UTC)
val weatherSlim =
  if (weatherSlim0.columns.contains("obs_hour_utc")) weatherSlim0
  else weatherSlim0.withColumn("obs_hour_utc", date_trunc("hour", col("obs_utc_ts")))

// =======================
// 2) Tables de support
// =======================

// 2.1) Table des fuseaux depuis la météo filtrée
val airports_tz =
  weatherSlim.select($"airport_id", $"tz").dropDuplicates()
    .withColumn("tz", coalesce($"tz", lit("UTC")))

// 2.2) Enrichit flights avec tz origine & destination + timestamps UTC + flight_key
val flightsEnriched =
  flightsSlim0
    // tz origine
    .join(airports_tz.withColumnRenamed("tz","origin_tz"), $"origin_airport_id" === $"airport_id", "left")
    .drop("airport_id")
    // tz destination
    .join(airports_tz.withColumnRenamed("tz","dest_tz"), $"dest_airport_id" === $"airport_id", "left")
    .drop("airport_id")
    .withColumn("origin_tz", coalesce(col("origin_tz"), lit("UTC")))
    .withColumn("dest_tz",   coalesce(col("dest_tz"),   lit("UTC")))
    // Timestamps UTC (à partir des timestamps locaux déjà présents)
    .withColumn("dep_ts_utc", to_utc_timestamp(col("CRS_DEP_TS"), col("origin_tz")))
    .withColumn("arr_ts_utc", to_utc_timestamp(col("CRS_ARR_TS"), col("dest_tz")))
    // Clef de vol (stable)
    .withColumn(
      "flight_key",
      concat_ws("|",
        date_format(col("FL_DATE").cast("timestamp"), "yyyy-MM-dd"),
        col("OP_CARRIER_AIRLINE_ID").cast("string"),
        col("OP_CARRIER_FL_NUM").cast("string"),
        col("origin_airport_id").cast("string"),
        col("dest_airport_id").cast("string"),
        lpad(col("CRS_DEP_TIME").cast("string"), 4, "0")
      )
    )

// ==========================================================
// 3) Helpers robustes aux colonnes météo manquantes (Wo/Wd)
// ==========================================================
def colIfExists(dfCols: Set[String], name: String, dt: DataType): Column =
  if (dfCols.contains(name)) col(name).cast(dt) else lit(null).cast(dt)

val wxColsSet = weatherSlim.columns.toSet

def buildWxStruct(prefix: String): Column =
  struct(
    col("offset").as("h_back"),
    col("obs_hour_utc").as(s"${prefix}_hour_utc"),
    colIfExists(wxColsSet, "DryBulbCelsius",   DoubleType).as(s"${prefix}_temp_c"),
    colIfExists(wxColsSet, "RelativeHumidity", DoubleType).as(s"${prefix}_rh"),
    colIfExists(wxColsSet, "WindSpeed",        DoubleType).as(s"${prefix}_wind_spd"),
    colIfExists(wxColsSet, "WindDirection",    DoubleType).as(s"${prefix}_wind_dir"),
    colIfExists(wxColsSet, "SeaLevelPressure", DoubleType).as(s"${prefix}_slp"),
    colIfExists(wxColsSet, "Visibility",       DoubleType).as(s"${prefix}_vis"),
    colIfExists(wxColsSet, "Altimeter",        DoubleType).as(s"${prefix}_alt"),
    colIfExists(wxColsSet, "HourlyPrecip",     DoubleType).as(s"${prefix}_precip"),
    colIfExists(wxColsSet, "WeatherType",      StringType ).as(s"${prefix}_wx")
  )

// ======================================================
// 4) Jointure 2 temps — ORIGINE (MAP / PARTITION / REDUCE)
// ======================================================

// --- MAP (vols → offsets 0..12 + join_hour en UTC sans timestampadd) ---
val flightsOriginOffsets =
  flightsEnriched
    .select(
      $"flight_key", $"origin_airport_id".as("airport_id"),
      $"dep_ts_utc", $"ARR_DELAY_NEW", $"year", $"month",
      $"FL_DATE", $"OP_CARRIER_AIRLINE_ID", $"OP_CARRIER_FL_NUM",
      $"dest_airport_id", $"CRS_DEP_TIME", $"CRS_DEP_TS", $"CRS_ARR_TS"
    )
    .withColumn("offset", explode(sequence(lit(0), lit(12))))
    // floor(epoch/3600) puis -offset, puis reconversion en timestamp
    .withColumn("dep_hour_epoch", floor(unix_timestamp($"dep_ts_utc")/3600))
    .withColumn("join_hour_epoch", ($"dep_hour_epoch" - $"offset") * 3600)
    .drop("dep_hour_epoch")
    .withColumn("join_hour", from_unixtime($"join_hour_epoch").cast("timestamp"))

// --- PARTITION (co-localiser (airport_id, hour)) ---
val flightsOriginPart = flightsOriginOffsets.repartition(col("airport_id"), col("join_hour"))

// Prépare weatherPartOrigin (évite l’expansion varargs avec : _*)
val baseWxColsO = Seq($"airport_id", $"obs_utc_ts", $"obs_hour_utc")
val otherWxColsO = weatherSlim.columns.diff(Seq("airport_id","obs_utc_ts","obs_hour_utc")).map(col)
val allWxColsO   = baseWxColsO ++ otherWxColsO

val weatherPartOrigin = weatherSlim.select(allWxColsO: _*)
  .repartition(col("airport_id"), col("obs_hour_utc"))

// --- REDUCE (join sur heure exacte + agrégation triée) ---
val joinedOrigin =
  flightsOriginPart.join(weatherPartOrigin, Seq("airport_id"), "left")
                   .filter(col("obs_hour_utc") === col("join_hour"))

val WoAgg =
  joinedOrigin.groupBy($"flight_key")
    .agg( sort_array(collect_list(buildWxStruct("o")), asc = true).as("Wo_raw") )

// ======================================================
// 5) Jointure 2 temps — DESTINATION (idem ORIGINE)
// ======================================================

val flightsDestOffsets =
  flightsEnriched
    .select(
      $"flight_key", $"dest_airport_id".as("airport_id"),
      $"arr_ts_utc", $"ARR_DELAY_NEW", $"year", $"month",
      $"FL_DATE", $"OP_CARRIER_AIRLINE_ID", $"OP_CARRIER_FL_NUM",
      $"origin_airport_id", $"CRS_DEP_TIME", $"CRS_DEP_TS", $"CRS_ARR_TS"
    )
    .withColumn("offset", explode(sequence(lit(0), lit(12))))
    .withColumn("arr_hour_epoch", floor(unix_timestamp($"arr_ts_utc")/3600))
    .withColumn("join_hour_epoch", ($"arr_hour_epoch" - $"offset") * 3600)
    .drop("arr_hour_epoch")
    .withColumn("join_hour", from_unixtime($"join_hour_epoch").cast("timestamp"))

val flightsDestPart = flightsDestOffsets.repartition(col("airport_id"), col("join_hour"))

// Prépare weatherPartDest (même technique pour éviter : _*)
val baseWxColsD = Seq($"airport_id", $"obs_utc_ts", $"obs_hour_utc")
val otherWxColsD = weatherSlim.columns.diff(Seq("airport_id","obs_utc_ts","obs_hour_utc")).map(col)
val allWxColsD   = baseWxColsD ++ otherWxColsD

val weatherPartDest = weatherSlim.select(allWxColsD: _*)
  .repartition(col("airport_id"), col("obs_hour_utc"))

val joinedDest =
  flightsDestPart.join(weatherPartDest, Seq("airport_id"), "left")
                 .filter(col("obs_hour_utc") === col("join_hour"))

val WdAgg =
  joinedDest.groupBy($"flight_key")
    .agg( sort_array(collect_list(buildWxStruct("d")), asc = true).as("Wd_raw") )

// ======================================================
// 6) Assemblage JT = {F, Wo, Wd, C}
// ======================================================

// F = struct des infos vol (ajuste la liste si besoin)
val Fcols = struct(
  $"FL_DATE", $"OP_CARRIER_AIRLINE_ID", $"OP_CARRIER_FL_NUM",
  $"origin_airport_id", $"dest_airport_id",
  $"CRS_DEP_TIME", $"CRS_DEP_TS", $"CRS_ARR_TS"
).as("F")

// C = 1 si ARR_DELAY_NEW >= Th, sinon 0
val labelCol = when($"ARR_DELAY_NEW" >= lit(thMinutes), lit(1)).otherwise(lit(0)).cast("int").as("C")

val flightsForJT =
  flightsEnriched.select($"flight_key", Fcols, $"ARR_DELAY_NEW", $"year", $"month")

val JT =
  flightsForJT
    .join(WoAgg, Seq("flight_key"), "left")
    .join(WdAgg, Seq("flight_key"), "left")
    .select(
      $"F",
      $"Wo_raw".as("Wo"),
      $"Wd_raw".as("Wd"),
      labelCol,
      $"year", $"month"
    )

// =======================
// 7) Écriture Delta
// =======================
JT.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema","true")
  .partitionBy("year","month")
  .save(jtPath)

println(s"JT écrit → $jtPath")
println(s"Lignes JT: " + spark.read.format("delta").load(jtPath).count)


// COMMAND ----------

// MAGIC %md
// MAGIC