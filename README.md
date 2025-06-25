# Flight_project_EMIASD6
Projet FLIGHT

Ce projet a pour objet de prédire les retards de vol à l'arrivée pour cause de météo, à partir de fichiers traçant l'activité aérienne intérieur aux USA de janvier 2012 à décembre 2014 et des données météorologiques recueillies au cours de cette période.

Les volumes de données étant importants, la programmation du modèle se fera en traitement distribué en langage scala.

Ce projet est structuré autour de 3 pipelines : Préparation des données, entraînement d'un modèle, instantiation du modèle

Dans une première phase, le développement et les tests se font sur un sous ensemble des données avant de passer à l'échelle sur un cloud.

Les retards sur les vols sont catégorisés par les autorités américaines en 5 catégories :

- $compagnie aérienne$ : la cause revient à la compagnie (problème d'équipage, de bagage, de maintenance, nettoyage, fueling)
- $Arrivée tardive de l'avion$ qui doit faire le vol en raison d'un retard de celui-ci sur le vol précédent
- $National Aviation System$ : retard dû aux décisions de l'autorité de contrôle du trafic aérien (densité du trafic, météo non extrême, opérations sur l'aéroport)
- $Conditions météos extrêmes$
- $Sécurité$ : évacuation d'un terminal, réembarquement d'un vol pour un contrôle sécurité, contrôles de sécurité en panne ou surchargés

A noter qu'en dehors du cas de météo extrême, une partie des retards NAS est due à la météo, ainsi que pour les arrivées tardives de l'avion.
