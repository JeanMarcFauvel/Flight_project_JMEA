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

// Lister un dossier
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

val weatherTxt = spark.read.text("/FileStore/tables/weather/201201hourly.txt")
weatherTxt.printSchema()


// COMMAND ----------

// MAGIC %md
// MAGIC Vérification de l'exhaustivité des chargements des fichiers de vols.

// COMMAND ----------

println("Contenu de /FileStore/tables/flights/")
dbutils.fs.ls("/FileStore/tables/flights").foreach(f => println(f.name))


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
// MAGIC Afficher le schéma d'un fichier vols

// COMMAND ----------

val flightCsv = spark.read
  .option("header", true)
  .option("inferSchema", true)
  .csv("/FileStore/tables/flights/201201.csv")

flightCsv.printSchema()
