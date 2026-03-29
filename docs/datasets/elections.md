# Données Électorales

Ce document détaille les décisions d'ingestion et de transformation pour les résultats des élections présidentielles 2022 (T1 et T2) à Lyon.

## 📊 Source des Données

- **Producteur** : Ministère de l'Intérieur.
- **Diffuseur** : [Données définitives de l'élection présidentielle 2022 (data.gouv.fr)](https://www.data.gouv.fr/datasets/election-presidentielle-des-10-et-24-avril-2022-resultats-definitifs-du-1er-tour)
- **Granularité** : Bureau de vote.
- **Périmètre** : Ville de Lyon (Code INSEE 69123).

## 🛠️ Choix de Traitement (ETL)

### 1. Ingestion Bronze

- **Format** : Conversion du CSV brut en **Parquet**.
- **Décision** : Nous conservons l'intégralité des colonnes d'origine (même si redondantes) pour garantir une "vérité brute" immuable.
- **Métadonnées** : Ajout systématique du nom du fichier source et date de traitement.

### 2. Passage en Silver (Normalisation & Pivot)

- **Gestion Dynamique du Scrutin** : Notre pipeline est conçu pour être générique. Le script calcule automatiquement le nombre de candidats présents dans le fichier source (12 au premier tour, 2 au second), ce qui nous permet de traiter n'importe quel scrutin sans intervention manuelle sur le code.
- **Normalisation Politique** : Création d'un référentiel politique (`mapping_politique.csv`) pour normaliser les candidats et leur affecter un bloc idéologique.
  Pour cela, plusieurs sources distinctes :
  - [Grille bloc de clivage](https://www.legifrance.gouv.fr/download/pdf/circ?id=45336)
  - [Décision conseil d'état - contestation bloc de clivage](https://www.legifrance.gouv.fr/ceta/id/CETATEXT000049267171)

  - **Pivot (Wide to Long)** : Transformation du format "Large" (12 candidats en colonnes) en format "Long" (1 ligne par candidat et par bureau). ["Each variable is a column, each observation is a row and each type of obervationnal unit is a table"](https://www.jstatsoft.org/article/view/v059i10)
  - **Repérage des Arrondissements** : Mise en place d'une règle d'extraction basée sur le code du bureau de vote pour identifier automatiquement l'arrondissement lyonnais concerné (1er au 9ème). Préfixe = arrondissement et suffixe = N° du bureau de vote.
  - **Standardisation & Typage** : Harmonisation de la casse des noms et prénoms pour fiabiliser les rapprochements de données, et conversion systématique des métriques (voix, inscrits, votants) en nombres entiers pour sécuriser les futurs calculs en couche Gold.
  - **Nettoyage** : Suppression des colonnes techniques (ex: codes préfecture) et des pourcentages pré-calculés pour ne conserver que la "Source of Truth" numérique.

### 3. Modélisation Gold

- **Objectif** : Création d'un schéma en étoile (BI) ou en constellation et agréger les tables en "one big table".
- **Justification** : La Gold utilise la donnée normalisée de la Silver pour produire des indicateurs agrégés (ex: Score par bloc idéologique par arrondissement) prêts pour la visualisation ou l'entraînement du modèle.

## ✅ Validation de la Qualité

- **Test d'Intégrité** : `Somme(Voix par Candidat) == Total Exprimes`.
- **Validation Géographique** : Vérification que tous les codes INSEE correspondent strictement aux arrondissements de Lyon (69381 à 69389).
