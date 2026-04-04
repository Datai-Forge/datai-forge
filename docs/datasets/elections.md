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

### 2. Passage en Silver

- **Format Long (Standard Industriel)** : En Silver, les données sont transformées du format "Large" (candidats en colonnes) vers le format "Long" (1 ligne par candidat par bureau).
  - **Justification Technique** : Garantit un schéma fixe et stable quel que soit le nombre de candidats (T1 vs T2), facilite l'intégration de nouveaux scrutins (législatives, 2017) et simplifie drastiquement le mapping politique (une seule jointure au lieu de 12).
- **Typologie des Bureaux (Nouveau)** : Création d'une colonne `type_bureau` pour distinguer les bureaux de vote selon leur représentativité territoriale.
  - **BUREAU ORDINAIRE** : Bureaux classiques de quartier (la grande majorité).
  - **BUREAU ADMINISTRATIF** : Bureaux spécifiques (ex: bureau 0001) regroupant des populations nomades (SDF, détenus, gens du voyage).
  - **Justification ML** : Cette typologie permet d'isoler en couche Gold ML les bureaux administratifs dont le signal électoral est décorrélé de la sociologie réelle du quartier (ex: 94% d'abstention au bureau 0001), évitant ainsi de biaiser l'apprentissage du modèle 2027.
- **Alignement Géographique (Code INSEE)** : Injection systématique du **Code INSEE de l'arrondissement** (69381 à 69389) dès la couche Silver.
  - **Bénéfice** : Aligne immédiatement les élections sur le référentiel des données socio-économiques et de sécurité, sécurisant ainsi les futures jointures Gold.
- **Normalisation Politique** : Utilisation d'un référentiel (`mapping_politique.csv`) pour affecter à chaque candidat un parti et un bloc analytique (Gauche, Droite, Centre, etc.).
- **Nettoyage & Typage** : Conversion des métriques en entiers et normalisation de la casse des noms.

### 3. Modélisation Gold (Wide & OBT)

- **Dualité BI vs ML** :
  - **Gold BI** : Conserve le format Long pour permettre des rapports flexibles (filtres par bloc, par parti).
  - **Gold ML (One Big Table)** : Repivote les données en format "Wide" (1 ligne par bureau, scores des candidats en colonnes).
  - **Filtrage Qualité** : Seuls les bureaux de type `ORDINAIRE` sont conservés dans l'OBT ML pour garantir un signal pur lié au territoire.
- **Justification ML** : Un modèle de Machine Learning nécessite un vecteur de caractéristiques (Features) complet sur une seule ligne pour comparer les poids des candidats et prédire le vainqueur (Target) pour une observation donnée.

## ✅ Validation de la Qualité

- **Test d'Intégrité** : `Somme(Voix par Candidat) == Total Exprimes`.
- **Validation Géographique** : Vérification que les codes INSEE d'arrondissement sont compris entre 69381 et 69389.
- **Validation Typologique** : Test automatisé vérifiant que le bureau 0001 est bien tagué `ADMINISTRATIF` et rattaché au 1er arrondissement (69381).
