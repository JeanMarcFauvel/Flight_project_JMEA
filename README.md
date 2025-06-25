# Flight_project_EMIASD6
Projet FLIGHT

Ce projet a pour objet de prédire les retards de vol pour cause de météo, à partir de fichiers traçant l'activité aérienne intérieur aux USA de janvier 2012 à décembre 2014 et des données météorologiques recueillies au cours de cette période.

Les volumes de données étant importants, la programmation du modèle se fera en traitement distribué en langage scala.

Ce projet est structuré autour de 3 pipelines : Préparation des données, entraînement d'un modèle, instantiation du modèle

Dans une première phase, le développement et les tests se font sur un sous ensemble des données avant de passer à l'échelle sur un cloud.
