# Geospatial and Temporal Data Analysis on the New York City Taxi Trip DataFile
## Membres de l'équipe

* Capocasale Romain
* Freiburghaus Jonas
* Moulin Vincent

## Questions
### Statistiques descriptives

1. Identifier les temps de trajet court. Ce qui pourrait intérpréter comme une altercation entre les chauffeurs et le clients. Il s'agirait de compter le nombre d'éventuelles altercations par taxi (license).
2. Réaliser des cartes de chaleur en fonction des entrées et sorties du taxi pour tous les taxis. Ceci pourrait par exemple permettre d'identifier les lieux de travail et les domiciles.

### Régression

3. Prédire le tips d'un trajet par rapport aux données géospatiales, au temps et potentiellement d'autres variables

## 1. Description of the dataset (size, information it contains)

Les données ont été téléchargées en suivant les liens ci-dessous :

* [Trip Data](http://chriswhong.com/wp-content/uploads/2014/06/nycTaxiTripData2013.torrent)
* [Fare Data](http://chriswhong.com/wp-content/uploads/2014/06/nycTaxiFareData2013.torrent)

Nous avons choisi d'analyser les données pour le mois de janvier 2013.

### Fichier : trip_data_1.csv

En utilisant la commande `du -h trip_data_1.csv`, nous voyons qu'il occupe un volume de 2.3 GB.
Pour un total de 14776616 lignes y compris la ligne d'entête, la commande utilisé est : `wc -l trip_data_1.csv`.
Chaque ligne décrit un trajet.

Il y a 14 variables disponibles par ligne. Ces variables sont : 

* medallion
  * Identifiant d'un permis transférable (véhicule)
  * Variable catégorique
* hack_license
  * Identifiant d'un permis de taxis (chauffeur)
  * Variable catégorique
* vendor_id
  * Identifiant de l'entreprise de Taxi
  * Variable catégorique
  * Il y a 2 entreprises (CMT, VTS) `cat trip_data_1.csv | cut -d ',' -f 3 | sort | uniq -c`
    * CMT: 7450899 lignes
    * VTS: 7325716 lignes
* rate_code
  * Le type de facturation
  * Variable catégorique
  * Il y a 14 types de facturation dans le jeu de données
    * 0: 667 lignes
    * 1: 14456067 lignes
    * 2: 28 lignes
    * 3: 17655 lignes
    * 4: 22831 lignes
    * 5: 39889 lignes
    * 6: 315 lignes
    * 7: 2 lignes
    * 8: 10 lignes
    * 9: 1 ligne
    * 28: 2 lignes
    * 65: 1 ligne
    * 128: 4 lignes
    * 210: 11 lignes
* store_and_fwd_flag
  * Indique si le trajet a été gardé en mémoire avant d'être envoyé au serveur en cas d'absence de réseau
  * Variable catégorique binaire
* pickup_datetime
  * La date et l'heure à partir de laquelle la course commence à être facturée
  * Date, peut être utilisée comme variable continue, cyclique ou catégorique
* dropoff_datetime
  * La date et l'heure à partir de laquelle la course se termine (n'est plus facturée)
  * Date, peut être utilisée comme variable continue, cyclique ou catégorique
* passenger_count
  * Le nombre entier de passagers
  * Nombre entier
* trip_time_in_secs
  * La durée du trajet en seconde
  * Variable continue
* trip_distance
  * La distance du trajet en Miles
  * Variable continue
* pickup_longitude
  * La coordonnée longitude du lieu de départ
  * Variable continue
* pickup_latitude
  * La coordonnée latitude du lieu de départ
  * Variable continue
* dropoff_longitude
  * La coordonnée longitude du lieu d'arrivée
  * Variable continue
* dropoff_latitude
  * La coordonnée latitude du lieu d'arrivée
  * Variable continue

### Fichier: trip_fare_1.csv

Occupe un volume de 1.6 GB en mémoire pour 14776617 lignes.

Il y a 11 variables disponibles par ligne. Ces variables sont :

* medallion
* hack_license
* vendor_id
* pickup_datetime
* payment_type
  * Le type de payement
  * Variable catégorique
* fare_amount
  * Prix uniquement du trajet en fonction du temps et de la distance parcourue
  * Variable continue
* surcharge
  * Surcharge, par exemple s'il y a des valise ou surcharge nocturne
  * Variable continue
* mta_tax
  * Taxe pour la "Metropolitan Transit Authority"
  * Variable continue
* tip_amount
  * Le don
  * Variable continue
* tolls_amount
  * Les payages
  * Variable continue
* total_amount
  * Le coût total du trajet
  * Variable continue

**Remarque**: les 4 premières variables ont la même définition que pour le fichier trip_data_1.csv.

## 2. Description of the features used and any pre-processing to extract additional features - Romain


## 3. Questions for which you hope to get an answer from the analysis
### 3.1 Descriptive statistic - Vincent

### 3.2 Machine learning - Romain

### 3.3 Taxi profit

Pour calculer le profit nous avons cherché le coût total d'appartenance d'une voiture par Mile. Nous avons trouvé ce coût sur le site du ["Bureau of Transportation Statistics"](https://www.bts.gov/content/average-cost-owning-and-operating-automobilea-assuming-15000-vehicle-miles-year). Nous l'avons arrondi à 61 cents.

Nous calculons le coût du trajet comme étant :

$$\text{trip cost} = \text{distance} * \text{cost by miles}$$

Et finalement nous calculons le gain, comme ci-dessous :

$$\text{gain} = \text{total amount} - \text{trip cost}$$

Pour obtenir le profit par conducteur de taxi, nous groupons par licence. L'opération d'aggrégation est une somme.

De manière similaire, nous avons calculé le profit moyen par entreprise et par heure. Nous groupons donc sur 2 colones, en appliquant une moyenne.

Finalement, nous avons calculé le profit moyen par heure et par quartier. Ceci dans le but de savoir quel quartier est le plus profitable et si cela dépend de l'heure. Nous avons donc limité é l'aide d'une instruction `where` ce calcul aux trajets qui partaient d'un quartier et revenaient dans ce même quartier.

## 4. Algorithms you applied - Vincent


## 5. Optimisations you performed - Romain



## 6. Your approach to testing and evaluation

Pour tester des statistiques descriptives simples, comme le nombre de lignes et le type de données, nous avons utiliser les commandes unix dans un terminal. Des exemples sont donnés dans la section 1 de ce rapport. 

Pour évaluer les modèles de régression, nous avons utilisé comme métriques, la racine de l'erreur quadratique moyenne (Root Mean Squared Error (RMSE)).

$$\text{RMSE} = \sqrt{\frac{1}{n} \sum_{i=1}^n \left(\hat{y}_i - y_i \right)^2}$$

Ainsi que la déviation absolue moyenne (Mean Absolute Error (MAE))

$$\text{MAE} = \frac{1}{n} \sum_{i=1}^n |\hat{y}_i - y_i|$$

La déviation absolue moyenne est moins sensible aux valeurs extrêmes.

Afin d'avoir un modèle simple de comparaison, nous avons calculé la valeur moyenne du pourboire comme prédiction. Ainsi nos modèles de régression plus complexes se doivent au moins de faire mieux que la RMSE et le MAE de ce modèle simple.

## 7. Results you obtained - Vincent


## 8. Possible future enhancements

La visualisation de la densité de taxi dans la ville de New York pourrait être améliorée en utilisant une carte de chaleur superposée à un plan de la ville.

Pour la régression, une amélioration serait de considérer la variable catégorique `rate_code` qui a une influence directe sur le prix final et donc potentiellement sur le pourboire. Cependant, certain `rate_code`  ont peu d'exemple dans les données. Il faudrait donc soit les ignorer ou rassembler les `rate_code` rares en une seule catégorie si cela aurait du sens. Pour savoir si cela aurait du sens, nous nécessiterions une définition de ces `rate_code`.

Pour améliorer notre calcul du profit, il serait intéressant d'obtenir le coût par Mile des modèles des véhicules utilisés par les compagnies de taxi. De plus d'autres coûts devraient être pris en compte, tels que les frais administratif, de leur système d'information, des assurances, des licenses et certainement d'autres frais. Il s'agirait ensuite de trouver une formuler pour ajouter pour obtenir ces frais par Miles. Ce qui n'est pas une tâche facile.

