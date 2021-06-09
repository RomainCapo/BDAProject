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

## 1. Description of the dataset (size, information it contains) - Jonas


## 2. Description of the features used and any pre-processing to extract additional features - Romain


## 3. Questions for which you hope to get an answer from the analysis

### 3.1 Statistiques descriptives - Vincent

**1.1 Conflits potentiels**

L'idée ici est de détecter de potentiels conflits entre clients taxi. En effet, les taxis quittant Manhattan pour les banlieues sont parfois perdants, car il peut être plus difficile de toruver un client rapidement qui retourne sur Manhattan. De ce fait, certains taxis refusent des clients où leur demandent de sortir une fois que le client a donné sa destination. Cette pratique est cependant interdite, un chauffeur de taxi a l'obligation de prendre en charge un client peut importe sa destination.

On souhaite donc, parmis les données, mettre en évidence les chauffeurs de taxis qui aurait tendance à avoir recours à ces pratiques.

Pour commencer, nous faisons l'hypothèse suivante : les 5% des trajets les plus courts sont potentiellement des cas litigieux. Nous calculons donc le quantile 0.05 sur l'ensemble des temps de trajets à disposition dans les données.

On peut alors à présent faire un select sur sur nos données. On choisit de garder : le temps de trajet en secondes, la licence du taxi ainsi que le quartier. On indique une clause where qui spécifie que nous voulon des temps de trajets entre 0 secondes et la valeur du quartile calculé précédement, qui est de 60 secondes. On termine en groupant les trajets filtrés par licence et on applique un count. Voici le résultat obtenu:

![output_conflicts_by_licence](img/output_conflicts_by_licence.png)

Seuls les 20 premiers résultats, triés dans l'ordre décroissant sont affichés. On peut remarquer que certains chauffeur de taxi, selon le hash de leur licence, pourraient avoir tendance à potentiellement refuser des clients.

On peut ensuite lister le total de conflits potentiels, il est dans notre cas de 413654. On peut ensuite grouper cela par quartier :

![output_conflicts_by_borough](img/output_conflicts_by_borough.png)

On peut donc confirmer le fait que les taxis ont plutôt tendance à refuser des taxis depuis Manhattan. plus de 95% des conflits potentiels se trouvent dans cette zone.

**1.2 Baseline modèle pour le pourboire**

L'idée ici est de définir un modèle Baseline pour l'estimation du pourboire des clients pour les courses de taxis. Ce modèle est simplement la moyenne de tous les pourboires reversé par les clients. Nous allons ensuite appliquer cette moyenne comme prédiction du pourboire de chaque course, pour pouvoir calculer les métriques RMSE et MAE.

Ce modèle est très simple. Il a pour but déjà de décrire le pourboire moyen. Il aura comme objectif également d'avoir une base de travail que l'on pour comparer avec les modèles de regressions qui seront entraînés dans le chapitre 3.2. On pourra voir si nos modèles apprenent et permettent une estimation plus fine que ce modèle Baseline.

Voici les score que nous obtenons :

* **Mean of tip amount** : 1.2630700055188528
* **Baseline RMSE** : 2.019734868621055
* **Baseline MAE** : 1.3081345132729219

Le pourboire moyen est donc d'environ 1,26$.

Nous avons une RMSE et MAE plutôt élevée, ce qui est cohérent vu l faible complexité du modèle. Ces valeurs seront comparées à celles obtenues avec un modèle de ML entraîné.

**1.3 Bucketisation et affichage des données**

Nous allons à présent réaliser un graphe de type Scatter Plot des tips catégorisé. 

A l'aide de la classe ``Buketizer``, nous allons pouvoir catégoriser les tips de moins de 20$ dans N classes. Nous avons dans notre cas décidé de réaliser 15 blocs de tailles égales.

Ces points seront ensuite affichés sur un graphe et la légende sera leur catégorie. Les catégories iront donc de 1 à 15. Plus la valeur d'une classe est basse plus le pourboire est petit, et inversement.

Voici un exemple du graphe:

![tips_bucket](img/tips_bucket.png)

A noter que les pourboires sont afficher sur le graphe au point de dépôt du client. On voit qu'énormément de pourboire se situent dans la catégorie 1 et 2. Les classes 15 sont peut représentées. On peut cependant en remarquer certains. Ces pourboires assez élevés pourraient provenir d'homme d'affaires qui se rendent soit sur leur lieu de travail pour les points sur Manhattan ou lorsqu'ils rentrent chez eux dans les zone d'habitation situées en dehors de Manhattan.

On peut également noter que le regorupement en bas à droite est l'emplacement de l'aéroport JFK de New York, ce qui explique cette condensation de pourboire à un seul et même endroit, plutôt éloigné des autres.

### 3.2 Machine learning - Romain

### 3.3 Taxi profit - Jonas

## 4. Algorithmes appliqués - Vincent

Durant ce projet, certains algorithmes de Machine Learning ont été utilisés dans le but de résoudre une problématique sur les données des taxis New Yorkais.

C'est notamment le cas le chapitre 3.2, où différents algorithmes de la librairie Spark ML.

Algorithmes employés:

* Régression Linéaire
* Random Forest Regressor

Ces algorithmes ont été utilisés avec l'aide de différents composants Spark ML. Notamment les pipelines, les grid search et la validation croisée.

Ils ont été évalués avec les métriques RMSE et MAE.

## 5. Optimisations you performed - Romain


## 6. Your approach to testing and evaluation - Jonas


## 7. Resultats obtenus - Vincent

Nous avons pu, au travers de l'analyse du jeu de données des trajets de taxi de New York, répondre à des questions que nous nous posions sur cette problématique.

Nous avons été capable de :

* Détecter des conflits potentiels entre clients et chauffeurs selon les licences ou par quartier et tirer des conclusions
* Réaliser des modèles capables de prédire le pourboire selon divers charactéristiques
* Visualiser les pourboires selon leur range de prix et tirer des conclusions
* TODO

Les résultats obtenus sont plutôt cohérents et des conclusions logiques peuvent expliquer nos résultats.

## 8. Possible future enhancements - Jonas

