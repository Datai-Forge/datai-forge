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

## 📊 Schéma Décisionnel (Constellation d'Étoiles)

Le schéma ci-dessous présente la structure de la base de données décisionnelle, organisée verticalement pour une meilleure lecture des hiérarchies :

```mermaid
graph TD
    %% Style des nœuds
    classDef dim fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#01579b;
    classDef fact fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#e65100;
    classDef shared fill:#f3e5f5,stroke:#4a148c,stroke-width:2px,color:#4a148c;

    %% NIVEAU 1 : DIMENSIONS PIVOTS (PARTAGÉES)
    subgraph LVL1 [Axe Temps & Géographie]
        DG["<b>dim_geographie</b><hr/><i>ID Bureau, Arrondissement,<br/>ID Carreau, Commune</i>"]
        DT["<b>dim_temps</b><hr/><i>Année (PK), Décennie</i>"]
    end

    %% NIVEAU 2 : DIMENSIONS SPÉCIFIQUES
    subgraph LVL2 [Référentiels Métier]
        DC["<b>dim_candidats</b><hr/><i>Nom, Parti, Bloc Politique</i>"]
        DI["<b>dim_indicateurs_securite</b><hr/><i>Type de crime, Unité</i>"]
    end

    %% NIVEAU 3 : TABLES DE FAITS (EMPILEMENT VERTICAL)
    subgraph LVL3 [Mesures & Indicateurs]

        subgraph FAITS_ELEC [Élections]
            FV["<b>fact_votes</b><br/>(Voix)"]
            FP["<b>fact_participation</b><br/>(Inscrits, Abst.)"]
        end

        subgraph FAITS_SEC [Sécurité & Démographie]
            FS["<b>fact_securite</b><br/>(Nombre, Taux)"]
            FD["<b>fact_demographie_annuelle</b><br/>(Pop, Log.)"]
        end

        subgraph FAITS_SOC [Socio-Économique]
            FPa["<b>fact_pauvrete_200m</b><br/>(Revenus, Pauvreté)"]
        end
    end

    %% Relations Dimensions vers Faits
    DG ==> FV
    DG ==> FP
    DG ==> FS
    DG ==> FD
    DG ==> FPa

    DT -.-> FV
    DT -.-> FP
    DT -.-> FS
    DT -.-> FD
    DT -.-> FPa

    DC --> FV
    DI --> FS

    %% Application des styles
    class DG,DT shared;
    class DC,DI dim;
    class FV,FP,FS,FD,FPa fact;
```

### 🔍 Détails des Relations et Granularité

Le schéma repose sur l'utilisation de **clés de jointure naturelles et de substitution (SK)** pour lier les domaines :

1. **Axe Géographique (Dimension Pivot) :**
    - **Élections :** Liées par `id_bureau` (Code bureau de vote).
    - **Sécurité & Démographie :** Liées par `code_arrondissement` (Code INSEE 69381 à 69389).
    - **Social :** Lié par `sk_geographie` (Hash unique du carreau 200m).
    - _Note : Une table de correspondance interne permet de remonter du bureau de vote vers l'arrondissement._

2. **Axe Temporel (Dimension Pivot) :**
    - Toutes les tables de faits sont liées à `dim_temps` via l'année (`annee` ou `sk_temps`). Cela permet de comparer, par exemple, l'évolution de la délinquance en 2021 avec les revenus Insee de la même année.

3. **Axe Candidats :**
    - La table `fact_votes` est la seule liée à `dim_candidats`. Elle permet de filtrer les résultats par **Bloc Analytique** (Gauche, Droite, Centre, etc.) pour simplifier la lecture des tendances politiques.

4. **Axe Indicateurs Sécurité :**
    - La table `fact_securite` utilise `id_indicateur` pour distinguer les 15 types de crimes et délits suivis (ex: Cambriolages, Vols avec violence).

---

## ✅ Exécution du Flux

!!! tip "Mise en œuvre pratique"
Pour exécuter l'intégralité de ce flux de manière automatisée (de l'ingestion brute à la couche Gold), consultez la section sur le **[Lancement Global de la Pipeline](onboarding.md#lancement-global-de-la-pipeline)**.
