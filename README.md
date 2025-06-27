# Flight_project_EMIASD6
Projet FLIGHT

Ce projet a pour objet de prédire les retards de vol à l'arrivée pour cause de météo, à partir de fichiers traçant l'activité aérienne intérieur aux USA de janvier 2012 à décembre 2014 et des données météorologiques recueillies au cours de cette période.

Les volumes de données étant importants, la programmation du modèle se fera en traitement distribué en langage scala.

Ce projet est structuré autour de 3 pipelines : Préparation des données, entraînement d'un modèle, instantiation du modèle

Dans une première phase, le développement et les tests se font sur un sous ensemble des données avant de passer à l'échelle sur un cloud.

Les retards sur les vols sont catégorisés par les autorités américaines en 5 catégories :

- **Compagnie aérienne** : la cause revient à la compagnie (problème d'équipage, de bagage, de maintenance, nettoyage, fueling)
- **Arrivée tardive de l'avion** qui doit faire le vol en raison d'un retard de celui-ci sur le vol précédent
- **National Aviation System** : retard dû aux décisions de l'autorité de contrôle du trafic aérien (densité du trafic, météo non extrême, opérations sur l'aéroport)
- **Conditions météos extrêmes**
- **Sécurité** : évacuation d'un terminal, réembarquement d'un vol pour un contrôle sécurité, contrôles de sécurité en panne ou surchargés

A noter qu'en dehors du cas de météo extrême, une partie des retards NAS est due à la météo, ainsi que pour les arrivées tardives de l'avion. Au final environ le tiers des retards de vols sont dus à la météo.

**A noter**

La période 2012 - 2014 l'année 2012 a été l'année la plus chaude jamais enregistrée depuis 1895, marquée par : une sècheresse historique (61% du territoire touché), l'ouragan Sandy en octobre 2012 et des vagues de chaleur intenses en juillet (34 000 records de température batues).

Les phénomènes météorologiques qui causent le plus de retards des vols sont : 

- **la mauvaise visibilité** (brouillard, neige, précipitations) car elle complique les décollages et atterrissages. Principale case des retards
- **les orages** représentent un danger majeur (cisaillement de vent, grêle, microburst). Ils peuvent entrainer l'ajustement ou la suspension des opérations de vol
- **le vent fort** rend décollage et atterrissage risqué
- **le givrage / neige** la formation de glace nécessite des procédures de dégivrage qui allongent le temps de préparation au sol.
- **le froid extrême** ralentit les opérations et peut provoquer des retard liés à la maintenance ou la sécurité.
- **les vagues de chaleur** peuvent clouer les avions au sol, car l'air chaud réduit la portance et la performance des moteurs.
- mais aussi **les turbulences** et **les tempêtes de sable ou de poussière**
