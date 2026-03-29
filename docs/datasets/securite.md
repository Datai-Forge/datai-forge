# Données de Sécurité

Ce document détaille les décisions d'ingestion et de transformation pour les indicateurs de délinquance (SSMSI) croisés avec la ville de Lyon.

## 📊 Source des Données

- **Producteur** : Service Statistique Ministériel de la Sécurité Intérieure (SSMSI - Ministère de l'Intérieur).
- **Diffuseur** : [data.gouv.fr](https://www.data.gouv.fr/datasets/bases-statistiques-communale-departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-et-la-gendarmerie-nationales?resource_id=6252a84c-6b9e-4415-a743-fc6a631877bb)
- **Granularité** : Communale (Codes INSEE arrondissements).
- **Périmètre** : Ville de Lyon.

## 🛠️ Choix de Traitement (ETL)

### 1. Ingestion Bronze

- **Action** : Filtrage immédiat sur le code commune de Lyon (`69123`) dans la base nationale.
- **Format** : Apache Parquet pour optimiser les performances de lecture Spark.

### 2. Passage en Silver (Normalisation & Intégrité)

- **Gestion du Secret Statistique (ndiff)** : Le SSMSI masque certaines valeurs pour confidentialité. Nous utilisons une stratégie de **fallback automatique** sur les colonnes de complément (estimations) pour garantir une continuité de la donnée pour le Machine Learning.
- **Normalisation Temporelle** : Conversion de l'année brute en objet `Date` (1er janvier) pour assurer la compatibilité avec les fonctions de séries temporelles de Spark.
- **Précision Géographique** : Utilisation d'une **Regex** (`^6938[1-9]$`) pour identifier et isoler strictement les 9 arrondissements de Lyon à partir des codes géographiques 2025.

### 3. Modélisation Gold

- **Objectif** : Transformation des volumes de délits en **Taux pour 1000 habitants** (basé sur la population INSEE intégrée au dataset) pour permettre une comparaison objective de la sécurité entre les arrondissements.

## ✅ Validation de la Qualité

- **Audit des Nuls** : Vérification que le remplacement des valeurs `ndiff` par les estimations ministérielles ne crée pas d'aberrations statistiques.
- **Plage Temporelle** : Filtrage strict sur la période 2017-2022 pour correspondre au cycle électoral précédent.

- **Intégrité Géographique** : Filtrage strict sur le périmètre de la commune.
