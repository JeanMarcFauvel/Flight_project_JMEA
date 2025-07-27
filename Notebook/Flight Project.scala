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
// MAGIC Nous avons 8 fichiers météo chargés à l'adresse /FileStore/tables/weather. Nous allons examiner le schéma du premier fichier.

// COMMAND ----------

val weatherTxt = spark.read.text("/FileStore/tables/weather/201201hourly.txt")
weatherTxt.printSchema()


// COMMAND ----------

// MAGIC %md
// MAGIC Les fichiers météos étant des fichiers txt, le  schéma du fichier raw n'est constitué que d'une chaîne de caractères.

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
// MAGIC Afficher le schéma d'un fichier vols

// COMMAND ----------

val flightCsv = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("/FileStore/tables/flights/201201.csv")

flightCsv.printSchema()


// COMMAND ----------

// MAGIC %md
// MAGIC Afficher les 20 premières lignes d'un fichier vols

// COMMAND ----------

// Lire un fichier CSV avec en-tête
val flightCsv = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("/FileStore/tables/flights/201201.csv")

// Afficher les 20 premières lignes
flightCsv.show(20, truncate = false)


// COMMAND ----------

// MAGIC %md
// MAGIC ###Analyse des fichiers météo

// COMMAND ----------

// MAGIC %md
// MAGIC Distribution des tailles de lignes des fichiers .txt météo avant de parser
// MAGIC
// MAGIC Compte tenu du volume, on commence par un fichier : 201201hourly.txt

// COMMAND ----------

// Lire toutes les lignes du fichier de janvier 2012
val rdd = spark.sparkContext.textFile("/FileStore/tables/weather/201201hourly.txt")

// Calculer la longueur de chaque ligne
val lengths = rdd.map(_.length)

// Statistiques globales
val minLen = lengths.min()
val maxLen = lengths.max()
val meanLen = lengths.mean()
val total = lengths.count()

println(s"Total lignes     : $total")
println(s"Longueur min     : $minLen")
println(s"Longueur max     : $maxLen")
println(f"Longueur moyenne : $meanLen%.2f")

// Fréquences des longueurs
val lengthFreq = lengths.map(len => (len, 1L)).reduceByKey(_ + _).sortByKey()

println("Répartition des longueurs de lignes :")
lengthFreq.collect().foreach { case (len, count) =>
  println(f"- Longueur $len%3d : $count lignes")
}


// COMMAND ----------

// MAGIC %md
// MAGIC Nous constatons que les lignes des fichiers météo sont de tailles variables, ce qui laisse entendre que tous les champs ne sont pas renseignés sur toutes les lignes. Il manque très probablement un grand nombre de données.
// MAGIC
// MAGIC Nous observons aussi qu'une ligne a la longueur maximale de 698. Nous allons vérifier que cette ligne correspond à la lignes d'en-tête du fichier météo. Si tel est le cas, la longueur maximum des lignes de champs météo est de 175.
// MAGIC
// MAGIC J'ai auparavant fait ce contrôle sur les 8 fichiers et ai pu constater que j'avais bien 8 lignes de longueur 698 corespondants aux 8 en-têtes.

// COMMAND ----------

// Lire toutes les lignes de tous les fichiers météo
// val rdd = spark.sparkContext.textFile("/FileStore/tables/weather/*.txt")

// Filtrer les lignes de longueur exacte = 698
val headers = rdd.filter(_.length == 698)

// Afficher les 8 lignes
headers.collect().foreach(println)


// COMMAND ----------

// MAGIC %md
// MAGIC Nous avons confirmation que a ligne de longueur 698 est une en-tête de colonnes.
// MAGIC
// MAGIC Pour toutes les lignes, les champs sont séparés par des virgules comme dans un csv. Nous allons les lire comme nous lirions des csv.

// COMMAND ----------

val rawRdd = spark.sparkContext.textFile("/FileStore/tables/weather/201201hourly.txt")

// filtre des lignes d'en-tête
val filteredRdd = rawRdd.filter(_.length < 500)

// séparation des champs par des virgules
val dataRdd = filteredRdd.map(_.split(",", -1))  // -1 pour garder les champs vides

// COMMAND ----------

// MAGIC %md
// MAGIC Analyse de la structure des lignes hors lignes d'en-tête

// COMMAND ----------

val lengthStats = dataRdd.map(_.length).map(n => (n, 1)).reduceByKey(_ + _).sortByKey()
lengthStats.collect().foreach { case (n, count) =>
  println(s"$n champs : $count lignes")
}


// COMMAND ----------

// MAGIC %md
// MAGIC Nous avons confirmation qu'en dehors des lignes d'en-tête, les lignes de champs sont tous constitués de 44 champs. Mais nous avons vu précédemment qu'il est très probables que les valeurs de ces champs soient manquantes pour nombre de ces lignes.

// COMMAND ----------

// MAGIC %md
// MAGIC Nous allons utiliser les lignes d'en-tête pour créer le schéma d'un dataframe spark.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

// 1. Lire toutes les lignes brutes
val rdd = spark.sparkContext.textFile("/FileStore/tables/weather/201201hourly.txt")

// 2. Isoler les lignes d'en-tête (de longueur 698)
val headersRdd = rdd.filter(_.length == 698)

// 3. Extraire les noms de colonnes depuis la première ligne
val headerLine = headersRdd.take(1)(0)
val columnNames = headerLine.split(",", -1).map(_.trim)

// 4. Construire dynamiquement le StructType
val schema = StructType(columnNames.map(name => StructField(name, StringType, nullable = true)))

// (Optionnel) Afficher les noms pour vérification
println(s"${columnNames.length} colonnes détectées :")
columnNames.foreach(println)


// COMMAND ----------

// MAGIC %md
// MAGIC Création du dataframe

// COMMAND ----------

// 5. Lire toutes les lignes sauf les en-têtes (longueur < 698)
val dataLines = rdd.filter(_.length < 698)

// 6. Transformer les lignes en Row
val rows = dataLines
  .map(_.split(",", -1))
  .filter(_.length == columnNames.length)
  .map(arr => Row.fromSeq(arr.toSeq))

// 7. Créer un DataFrame structuré
val df = spark.createDataFrame(rows, schema)

// 8. Vérification
df.show(10, truncate = false)
df.printSchema()


// COMMAND ----------

// MAGIC %md
// MAGIC Analyse des **valeurs manquantes** dans les fichiers météo. Compte tenu de la taille des fichiers, nous prenons le parti de commencer l'analyse sur un fichier pour définir un pipeline de préprocessing que nous pourrons ensuite appliquer à l'ensemble des fichiers.

// COMMAND ----------

import org.apache.spark.sql.functions._

// Nombre total de lignes
val totalRows = df.count()

// Statistiques sur les valeurs manquantes par colonne
val nullStats = df.columns.map { colName =>
  val nullCount = df.filter(
    col(colName).isNull || trim(col(colName)) === "" || col(colName) === "M"
  ).count()

  val missingRate = nullCount.toDouble / totalRows * 100
  (colName, nullCount, f"$missingRate%.2f%%")
}

// Conversion en DataFrame pour affichage propre
val nullDF = nullStats.toSeq.toDF("Column", "MissingCount", "MissingRate")

// Affichage trié par nombre de valeurs manquantes
nullDF.orderBy(desc("MissingCount")).show(50, truncate = false)


// COMMAND ----------

// MAGIC %md
// MAGIC **Analyse des valeurs manquantes :**
// MAGIC
// MAGIC Il semble que toutes les colonnes "flag" ont des valeurs à plus de 99% manquantes. **Sous réserve** qu'il en soit de même sur les 7 autres fichiers, on pourra envisager de supprimer ces colonnes.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Création de la tables de correspondance airportId / wban filtrées sur les aéroports d'un fichiers flight (201201)
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC Dans le cadre de la recherche à limiter le volume de données au strict nécessaire à l'apprentissge, nous allons évaluer l'impact de réduire les données météos aux capteurs correspondants aux aéroports d'origine et de destination.
// MAGIC
// MAGIC Le fichier wban_airport_timezone.csv nous donne une correspondance entre les identifiants de capteurs météo et les Id des aéroports. Nous allons tester sur le mois de janvier 2012 le nombre de lignes météos correspondant aux aéroports d'origine et destination des vols du même mois par rapport aux nombre de ligne total de données météo de ce mois.

// COMMAND ----------

import org.apache.spark.sql.functions._

// Chargement du fichier de correspondance wban_airport_timezone.csv
val airportWbanDf = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/wban_airport_timezone.csv")
  .select("AirportId", "WBAN")

// Chargement du DataFrame des vols (depuis le fichier CSV)
val flightsDf_1201 = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("/FileStore/tables/flights/201201.csv") 
  .select("ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID")

// Récupération des AirportId distincts depuis les vols (origine et destination)
val airportIdsFromFlightsDf = flightsDf_1201
  .select(col("ORIGIN_AIRPORT_ID").as("AirportId"))
  .union(flightsDf_1201.select(col("DEST_AIRPORT_ID").as("AirportId")))
  .distinct()

// Filtrage du fichier de correspondance
val filteredWbanAirportDf = airportWbanDf
  .join(airportIdsFromFlightsDf, Seq("AirportId"))

// Enregistrement du mapping filtré
filteredWbanAirportDf.write
  .option("header", "true")
  .csv("/FileStore/tables/filtered_wban_airport.csv")


// COMMAND ----------

// MAGIC %md
// MAGIC Comparaison de taille avant et après filtre

// COMMAND ----------

// Taille du fichier original
val originalCount = airportWbanDf.count()

// Taille du fichier filtré
val filteredCount = filteredWbanAirportDf.count()

println(s"Taille fichier original : $originalCount lignes")
println(s"Taille fichier filtré  : $filteredCount lignes")


// COMMAND ----------

// MAGIC %md
// MAGIC Nous constatons que le fichier des vols de janvier 2012 matche 80 de airportId (origine & destination) du fichier de correspondance avec les wban des capteurs météos.

// COMMAND ----------

// MAGIC %md
// MAGIC ###Réduction de taille d'un fichier weather
// MAGIC
// MAGIC Filtrage du fichier weather 201201hourly à partir des wban correspondant aux aéroports origine et destination du fichier flight 201201.

// COMMAND ----------

// création d'un set des wban idssu du fichier de correspondance filtré des vols O&D du 201201
val validWbans: Set[String] = filteredWbanAirportDf
  .select("WBAN")
  .distinct()
  .rdd
  .map(_.getInt(0).toString.trim)
  .collect()
  .toSet


// COMMAND ----------

val header = rdd.first()
val dataRdd = rdd.filter(_ != header)

// Filtre des lignes de dataRdd en ne conservant que les lignes dont le wban est présent dans le set validWbans
val filteredRdd = dataRdd.filter { line =>
  val cols = line.split(",")
  val wban = cols(0).trim // WBAN est la première colonne
  validWbans.contains(wban)
}

// (optionnel) réajout de l’en-tête
val finalFilteredRdd = spark.sparkContext.parallelize(Seq(header)) ++ filteredRdd


// COMMAND ----------

// MAGIC %md
// MAGIC Comparaison des tailles des rdd météo filtré et non filtré en nombre de lignes

// COMMAND ----------

val originalCount = rdd.count()
val filteredCount = finalFilteredRdd.count()

println(s"Lignes totales du fichier météo : $originalCount")
println(s"Lignes après filtrage : $filteredCount")
println(f"Réduction : ${(100.0 * (originalCount - filteredCount) / originalCount)}%.2f%%")


// COMMAND ----------

// MAGIC %md
// MAGIC ###Analyse
// MAGIC
// MAGIC En filtrant un fichier weather sur les aéroports on réduit sa taille de 94%. Peut-être est-ce la première tache à faire après avoir filtré les données de vol pour éliminer les vols annulés ou déroutés.