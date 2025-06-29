Ce répertoire contient toutes les données du projet

Lien vers les données Flight et Weather : https://www.dropbox.com/sh/iasq7frk6f58ptq/AAAzSmk6cusSNfqYNYsnLGIXa

Nous disposons de 36 mois d'activité des vol intérieurs aux USA du 01/01/2012 au 31/12/2014 avec un fichier csv par mois et 8 mois de données météorologiques 4 mois en 2012 01/05/07/09 et 4 mois en 2013 04/06/09/11

Le fichier excel data_flight_extract_mini10K a trois onglets. Chacun d'entre eux correspond à une extraction des trois fichiers mis à notre disposition.

A noter que les fichiers d'activité des aéoroports contiennent les informations d'heure de départ programmé (CRS_DEP_TIME), de temps de vol programmé (CRS_ELAPSE_TIME) et de retard en minutes (ARR_DELAY_NEW), mais par d'heure d'arrivée programmé, ni réelle.
CRS signifie : Computer Reservation System
La question se posera de savoir si on doit tenir compte de la météo de l'aéroport destination avant le départ du vol ou à son heure d'arrivée. On peut supposer que le retard est dû à un départ retardé du fait de la météo à destination...

ARR_DELAY_NEW semble être le label d'apprentissage.
