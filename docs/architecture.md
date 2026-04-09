# Architecture des Données

Le projet suit l'architecture de données **Médaillon**, permettant de garantir la qualité et la traçabilité des données de la source jusqu'au modèle de Machine Learning.

## 🏗️ Les 3 Couches du Projet

### 1. 🟤 Bronze (Raw)

- **Source** : Fichiers CSV bruts issus de data.gouv.fr et du SSMSI.
- **Traitement** : Ingestion "telle quelle".
- **Format** : Apache Parquet.
- **Objectif** : Conservation de la "Vérité Brute" avec ajout de métadonnées de lignage (`source_file`, `processing_timestamp`).

### 2. 🥈 Silver (Cleaned & Enriched)

- **Traitement** : Nettoyage (_Regex_), normalisation des types, filtrage géographique strict sur Lyon, imputation...
- **Enrichissement** : Jointure avec le référentiel politique (_Blocs idéologiques_). [Source des nuances et blocs politiques](https://economie-politique.org/tableau-des-partis-politiques-en-france/?utm_source=chatgpt.com#google_vignette)
- **Objectif** : Fournir une table "Propre" et stable, prête pour l'analyse. C'est la couche pivot pour la qualité.

### 3. 🥇 Gold (ML-Ready / BI)

- **Produit 1 : Gold BI** : Schéma en constellation d'étoiles (_Faits & Dimensions_) pour le reporting et la visualisation.
- **Produit 2 : Gold ML (One Big Table)** : Une table unique "plate" regroupant toutes les features. [Liste des features](./dictionnaires/features_ml.md)

#### 🔄 Pipeline Gold ML Incrémentale

Contrairement à une approche monolithique, la construction de la table finale pour le Machine Learning est **découpée en 3 étapes séquentielles** :

1. **Step 1 (Base)** : Création du socle électoral (_Pivotement des blocs politiques)_.
2. **Step 2 (Security)** : Enrichissement par les indicateurs de délinquance (_3 Piliers & Deltas_).
3. **Step 3 (Social)** : Enrichissement par les données socio-économiques Insee (_Revenus, Pauvreté ...)_.

#### ❓ Pourquoi une approche incrémentale ?

Nous avons opté pour ce design pour quatre raisons :

1. **Audit (Checkpoints)** : À chaque étape, un fichier Parquet intermédiaire est généré. Cela permet d'auditer la donnée à la fin de l'étape 2 sans avoir à relancer toute la chaîne.
2. **Debug ciblée** : Si une erreur de jointure survient sur les données Insee (Step 3), nous savons immédiatement que le problème est isolé dans ce script, sans impacter la logique électorale ou de sécurité.
3. **Modularité** : Si demain nous souhaitons ajouter une "Step 4 :Temps de présence médiatique" ou "Step 5 : Transports", il suffit de créer un nouveau script qui consomme la sortie de la dernière step, sans toucher au code existant.
4. **Optimisation des Ressources** : Spark gère mieux des transformations séquentielles sauvegardées sur disque que des jointures massives à 15 tables en une seule exécution, évitant ainsi les débordements de mémoire (OOM).

---

## 🔄 Flux de Données

```mermaid
graph TD
    subgraph "Sources"
        RAW[Données Brutes CSV]
    end

    subgraph "Traitement et raffinement "
        RAW --> B[Bronze Layer - Parquet]
        B --> S[Silver Layer - Cleaned]

        subgraph "Pipeline Gold ML Incrémentale"
            S --> G1[Gold ML Step 1: Élections]
            G1 --> G2[Gold ML Step 2: Sécurité]
            G2 --> G3[Gold ML Step 3: Social]
            G3 --> OBT[One Big Table ML]
        end

        subgraph "Couche Gold BI"
            S --> G_BI[Gold BI - Schéma en Étoile]
        end
    end

    subgraph "Serving & Reporting"
        G_BI -- "Synchronisation Gold" --> DB[(MySQL Lyon Decisional)]
        DB -- "SQL Queries" --> DASH[Dashboard Dash]
    end

    OBT --> MODEL[Modèle Prédictif ML]
```

---

!!! tip "Mise en œuvre pratique"
Pour exécuter l'intégralité de ce flux de manière automatisée (de l'ingestion brute à la couche Gold), consultez la section sur le **[Lancement Global de la Pipeline](onboarding.md#lancement-global-de-la-pipeline)**.
