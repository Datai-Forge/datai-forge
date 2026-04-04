# 🤖 Stratégie de Machine Learning

Ce document détaille la méthodologie scientifique et les choix d'ingénierie des données appliqués pour la construction du modèle prédictif des élections 2027.

## 🏗️ La "One Big Table" (OBT)

Le cœur de notre approche ML repose sur la construction d'une **Table Unique (OBT)**. Contrairement à la couche Silver qui est atomique et normalisée, l'OBT Gold ML est optimisée pour l'apprentissage statistique.

### Grain de l'Observation (Multi-niveaux)
- **Cibles (Target $Y$)** : Le **Bureau de Vote** par Tour (608 lignes). C'est la maille la plus fine pour capter le résultat électoral.
- **Variables (Features $X$)** : Les indicateurs territoriaux (Sécurité, Social) sont agrégés au niveau de l'**Arrondissement** (Code INSEE 69381-69389).
- **Réconciliation** : Chaque bureau de vote "hérite" des caractéristiques sociologiques de son arrondissement de rattachement via une jointure `N:1`.

---

## 🛠️ Pipeline de Construction Incrémentale

La construction de l'OBT finale est opérée en trois étapes séquentielles. Nous avons choisi cette architecture **incrémentale** (Step 1 ➔ Step 2 ➔ Step 3) plutôt qu'un bloc monolithique pour plusieurs raisons :

1.  **Auditabilité (Checkpoints)** : On génère un fichier Parquet intermédiaire après chaque enrichissement. On peut auditer l'état de la donnée sans tout recalculer.
2.  **Modularité** : Facilité d'ajouter un "Step 4" (ex: météo, transports) sans modifier le code existant.
3.  **Debuggabilité** : En cas d'erreur de jointure, le problème est immédiatement localisé dans le script concerné.

### Étape 1 : Le Socle Électoral (`gold_ml_step1_base.py`)
- **Action** : Pivotement des résultats du format Long (candidats) vers le format Wide (blocs politiques).
- **Nettoyage** : Exclusion systématique du bureau 0001 (Administratif) pour supprimer le bruit statistique.

| Catégorie | Colonnes ajoutées (Noms exacts) |
| :--- | :--- |
| **Identifiants** | `id_bureau`, `tour`, `code_insee_arrondissement` |
| **Features (X)** | `feat_abstention_pct`, `feat_participation_pct` |
| **Cibles (Y)** | `target_score_gauche_pct`, `target_score_centre_pct`, `target_score_droite_pct`, `target_score_extreme_droite_pct` |

### Étape 2 : Enrichissement Sécurité (`gold_ml_step2_enrichment_security.py`)
- **Action** : Agrégation des indicateurs SSMSI par **Arrondissement** en 3 Piliers (Violence, Propriété, Rue/Stup).
- **Dynamique** : Calcul des évolutions (Deltas) sur 3 et 5 ans.

| Catégorie | Colonnes ajoutées (Noms exacts) |
| :--- | :--- |
| **Piliers (Snapshot)** | `feat_secu_violence_2021`, `feat_secu_propriete_2021`, `feat_secu_rue_stup_2021` |
| **Dynamique (3 ans)** | `feat_secu_violence_delta_3ans`, `feat_secu_propriete_delta_3ans`, `feat_secu_rue_stup_delta_3ans` |
| **Dynamique (5 ans)** | `feat_secu_violence_delta_5ans`, `feat_secu_propriete_delta_5ans`, `feat_secu_rue_stup_delta_5ans` |

### Étape 3 : Enrichissement Socio-Économique (`gold_ml_step3_enrichment_social.py`)
- **Action** : Agrégation des indicateurs Insee Filosofi par **Arrondissement**.
- **Calcul** : Création de ratios (pauvreté, revenus) et des évolutions sur 5 ans.

| Catégorie | Colonnes ajoutées (Noms exacts) |
| :--- | :--- |
| **Structure (Snapshot)** | `feat_social_revenu_moyen`, `feat_social_taux_pauvrete`, `feat_social_pct_proprietaires`, `feat_social_pct_logements_sociaux` |
| **Dynamique (5 ans)** | `feat_social_delta_revenu_5ans`, `feat_social_delta_pauvrete_5ans` |

---

## 🗳️ Feature Engineering : Élections

### De l'Individu au Bloc Analytique
Nous avons fait le choix de **pivoter** les résultats électoraux pour passer d'une liste de candidats (Macron, Le Pen, etc.) à des **blocs politiques structurels** (Gauche, Centre, Droite, etc.).

- **Robustesse 2027** : Le modèle apprend la signature politique d'un territoire. Il reste valide même si les noms des candidats changent en 2027.
- **Normalisation** : Toutes les cibles ($Y$) sont exprimées en **pourcentage des suffrages exprimés** pour éliminer le biais de taille des bureaux de vote.

### Nettoyage du Signal
- **Typologie des Bureaux** : Nous excluons systématiquement les bureaux de type **ORDINAIRE**. Seuls les bureaux de quartier classiques sont conservés.
- **Raison** : Les bureaux **ADMINISTRATIFS** regroupent des populations nomades (SDF, détenus) dont le comportement électoral est un artefact administratif décorrélé de la sociologie du quartier physique.

---

## 🛡️ Feature Engineering : Sécurité

Plutôt que d'injecter les 14 indicateurs bruts du Ministère de l'Intérieur au risque de créer du bruit statistique (sur-apprentissage), nous les avons regroupés en **3 Familles d'Insécurité** (Piliers).

*Note : Pour la justification théorique et sociologique détaillée de ce choix (Théorie de François Abel et étude du SSMSI), consultez l'onglet [Choix des Données ML](./ml_choix_donnees.md).*

### Dynamique Temporelle
Pour chaque pilier, nous calculons deux types de variables :
1.  **Le Snapshot (2021)** : L'état des lieux immédiat pré-élection.
2.  **Les Deltas (3 ans et 5 ans)** : L'évolution de la délinquance. Cela permet au modèle de déterminer si l'électeur vote en fonction du niveau absolu de crime ou de son **sentiment de dégradation** (Dynamique).

---

## ✅ Validation et Audit

La construction de cette table est sécurisée par deux mécanismes :
1.  **Tests Automatisés** : Validation systématique du grain (608 lignes) et de l'intégrité mathématique des jointures.
2.  **Audit Notebook** : Un support visuel permettant de comparer les distributions de données avant et après chaque étape d'enrichissement.
