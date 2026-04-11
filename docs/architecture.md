# Architecture des Données

Le projet suit l'architecture de données **Médaillon**, permettant de garantir la qualité et la traçabilité des données de la source jusqu'au modèle de Machine Learning.

## 🏗️ Les 3 Couches du Projet

### 1. 🟤 Bronze (Raw)
- **Source** : Fichiers CSV bruts issus de data.gouv.fr et du SSMSI.
- **Traitement** : Ingestion "telle quelle".
- **Format** : Apache Parquet.

### 2. 🥈 Silver (Cleaned & Enriched)
- **Traitement** : Nettoyage (_Regex_), normalisation des types, filtrage géographique sur Lyon.
- **Enrichissement** : Jointure avec le référentiel politique (_Blocs idéologiques_).

### 3. 🥇 Gold (ML-Ready / BI)
- **Produit 1 : Gold BI** : Schéma en constellation d'étoiles pour le reporting.
- **Produit 2 : Gold ML (One Big Table)** : Table plate regroupant toutes les features.

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

## 📐 Modélisation du Système Décisionnel (OLAP)

Le passage d'une donnée brute à une information exploitable suit trois niveaux de modélisation.

### 1️⃣ Modèle Conceptuel (MCD) : Vision Métier
Le MCD identifie les **Processus Métiers** (bulles) et les **Axes d'analyse** (rectangles).

**Cardinalités :** En OLAP, les relations sont de type **1:N**. Une dimension (ex: une année) est liée à plusieurs faits (ex: des milliers de votes).

```mermaid
graph TD
    classDef fact fill:#ffcc80,stroke:#e65100,stroke-width:2px,color:#000;
    classDef dim fill:#90caf9,stroke:#01579b,stroke-width:2px,color:#000;

    GEO[Géographie]:::dim
    TMP[Temps]:::dim
    CAN[Candidat]:::dim
    IND[Indicateur Sécurité]:::dim

    V((Votes)):::fact
    P((Participation)):::fact
    S((Sécurité)):::fact
    D((Démographie)):::fact
    Pa((Pauvreté)):::fact

    GEO -- "1:N" --- V & P & S & D & Pa
    TMP -- "1:N" --- V & P & S & D & Pa
    CAN -- "1:N" --- V
    IND -- "1:N" --- S
```

### 2️⃣ Modèle Logique (MLD) : Schéma en Constellation
Le MLD définit la structure des tables. On utilise la **dénormalisation** pour maximiser la vitesse de lecture.

```mermaid
graph TD
    %% Style des nœuds
    classDef dim fill:#bbdefb,stroke:#01579b,stroke-width:2px,color:#000;
    classDef fact fill:#ffe0b2,stroke:#e65100,stroke-width:2px,color:#000;
    classDef shared fill:#e1bee7,stroke:#4a148c,stroke-width:2px,color:#000;

    subgraph LVL1 [Dimensions Pivots - Partagées]
        DG["<b>dim_geographie</b><hr/><i>ID Bureau, Arrondissement,<br/>ID Carreau, Commune</i>"]
        DT["<b>dim_temps</b><hr/><i>Année (PK), Décennie</i>"]
    end

    subgraph LVL2 [Référentiels Métier]
        DC["<b>dim_candidats</b><hr/><i>Nom, Parti, Bloc Politique</i>"]
        DI["<b>dim_indicateurs_securite</b><hr/><i>Type de crime, Unité</i>"]
    end

    subgraph LVL3 [Tables de Faits]
        subgraph FAITS_ELEC [Élections]
            FV["<b>fact_votes</b><br/>(Voix)"]
            FP["<b>fact_participation</b><br/>(Inscrits, Abst.)"]
        end
        subgraph FAITS_SEC [Sécurité & Démographie]
            FS["<b>fact_securite</b><br/>(Nombre, Taux)"]
            FD["<b>fact_demographie_annuelle</b><br/>(Pop, Log.)"]
        end
        subgraph FAITS_SOC [Social]
            FPa["<b>fact_pauvrete_200m</b><br/>(Revenus, Pauvreté)"]
        end
    end

    DG ==> FV & FP & FS & FD & FPa
    DT -.-> FV & FP & FS & FD & FPa
    DC --> FV
    DI --> FS

    class DG,DT shared; class DC,DI dim; class FV,FP,FS,FD,FPa fact;
```

### 3️⃣ Modèle Physique (MPD) : Optimisation du Stockage
Le MPD gère la performance via le stockage colonnaire et le partitionnement.

```mermaid
graph LR
    subgraph "Stockage Physique (Data Lake)"
        direction TB
        P1[Dossier: annee=2017] --> F1[Fichier Parquet - Colonnaire]
        P2[Dossier: annee=2019] --> F2[Fichier Parquet - Colonnaire]
        P3[Dossier: annee=2022] --> F3[Fichier Parquet - Colonnaire]
    end

    subgraph "Indexation (MySQL BI)"
        direction TB
        IDX1[Index B-Tree sur id_bureau]
        IDX2[Index B-Tree sur code_arrondissement]
    end
```

| Composant | Technologie | Justification BI |
| :--- | :--- | :--- |
| **Format** | **Apache Parquet** | Compression élevée et lecture ultra-rapide des colonnes de mesures. |
| **Partition** | **Année** | Évite le "Full Table Scan" lors des analyses temporelles. |
| **Index** | **Clés Substituts** | Jointures entières (INT) beaucoup plus rapides que sur des chaînes. |

---

### 🔍 Détails des Relations et Granularité

Le schéma repose sur l'utilisation de **clés de jointure naturelles et de substitution (SK)** :

1.  **Axe Géographique :** Lié par `id_bureau` (Élections), `code_arrondissement` (Sécurité) et `sk_geographie` (Social).
2.  **Axe Temporel :** Toutes les tables sont liées à `dim_temps` via l'année, permettant le croisement entre délinquance et résultats électoraux sur une même période.
3.  **Axe Candidats :** Permet d'analyser les votes par **Bloc Analytique** (Gauche, Droite, Centre, etc.).

---

!!! tip "Mise en œuvre pratique"
Pour exécuter l'intégralité de ce flux de manière automatisée, consultez le **[Guide de démarrage](onboarding.md)**.
