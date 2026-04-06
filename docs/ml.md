# 🤖 Stratégie de Machine Learning

Ce document détaille la méthodologie et les choix d'ingénierie des données appliqués pour la construction du modèle prédictif des élections 2027.

## 🏗️ La "One Big Table" (OBT)

Le cœur de notre approche ML repose sur la construction d'une **Table Unique (OBT)**. Contrairement à la couche Silver qui est atomique et normalisée, l'OBT Gold ML est optimisée pour l'apprentissage statistique.

### Grain de l'Observation (Multi-niveaux)

- **Cibles (Target $Y$)** : Le **Bureau de Vote** par Tour (608 lignes). C'est la maille la plus fine pour capter le résultat électoral.
- **Variables (Features)** : Les indicateurs territoriaux (Sécurité, Social) sont agrégés au niveau de l'**Arrondissement** (Code INSEE 69381-69389).
- **Réconciliation** : Chaque bureau de vote "hérite" des caractéristiques sociologiques de son arrondissement de rattachement via une jointure `N:1`.

---

## 🛠️ Pipeline de Construction Incrémentale

La construction de l'OBT finale est opérée en trois étapes séquentielles. Nous avons choisi cette architecture **incrémentale** (Step 1 ➔ Step 2 ➔ Step 3) plutôt qu'un bloc monolithique pour plusieurs raisons :

1. **Audit (Checkpoints)** : On génère un fichier Parquet intermédiaire après chaque enrichissement. On peut auditer l'état de la donnée sans tout recalculer.
2. **Modularité** : Facilité d'ajouter un "Step 4" (ex: temps de présence dans les médias, transports) sans modifier le code existant.
3. **Debuggabilité** : En cas d'erreur de jointure, le problème est immédiatement localisé dans le script concerné.

### Étape 1 : Le Socle Électoral (`gold_ml_step1_base.py`)

- **Action** : Pivotement des résultats du format Long (candidats) vers le format Wide (blocs politiques).
- **Nettoyage** : Exclusion systématique du bureau 0001 (Administratif) pour supprimer le bruit statistique. (ces bureaux sont décorrélés de la réalité urbaine et sociologique)

| Catégorie        | Colonnes ajoutées (Noms exacts)                                                                                    |
| :--------------- | :----------------------------------------------------------------------------------------------------------------- |
| **Identifiants** | `id_bureau`, `tour`, `code_insee_arrondissement`                                                                   |
| **Features (X)** | `feat_abstention_pct`, `feat_participation_pct`                                                                    |
| **Cibles (Y)**   | `target_score_gauche_pct`, `target_score_centre_pct`, `target_score_droite_pct`, `target_score_extreme_droite_pct` |

### Étape 2 : Enrichissement Sécurité (`gold_ml_step2_enrichment_security.py`)

- **Action** : Agrégation des indicateurs SSMSI par **Arrondissement** en 3 Piliers (Violence, Propriété, Rue/Stup).
- **Dynamique** : Calcul des évolutions (Deltas) sur 3 et 5 ans.

| Catégorie              | Colonnes ajoutées (Noms exacts)                                                                    |
| :--------------------- | :------------------------------------------------------------------------------------------------- |
| **Piliers (Snapshot)** | `feat_secu_violence_2021`, `feat_secu_propriete_2021`, `feat_secu_rue_stup_2021`                   |
| **Dynamique (3 ans)**  | `feat_secu_violence_delta_3ans`, `feat_secu_propriete_delta_3ans`, `feat_secu_rue_stup_delta_3ans` |
| **Dynamique (5 ans)**  | `feat_secu_violence_delta_5ans`, `feat_secu_propriete_delta_5ans`, `feat_secu_rue_stup_delta_5ans` |

### Étape 3 : Enrichissement Socio-Économique (`gold_ml_step3_enrichment_social.py`)

- **Action** : Agrégation des indicateurs Insee Filosofi par **Arrondissement**.
- **Calcul** : Création de ratios (pauvreté, revenus) et des évolutions sur 5 ans.

| Catégorie                | Colonnes ajoutées (Noms exacts)                                                                                               |
| :----------------------- | :---------------------------------------------------------------------------------------------------------------------------- |
| **Structure (Snapshot)** | `feat_social_revenu_moyen`, `feat_social_taux_pauvrete`, `feat_social_pct_proprietaires`, `feat_social_pct_logements_sociaux` |
| **Dynamique (5 ans)**    | `feat_social_delta_revenu_5ans`, `feat_social_delta_pauvrete_5ans`                                                            |

---

## 🗳️ Feature Engineering : Élections

### De l'Individu au Bloc Analytique

Nous avons fait le choix de **pivoter** les résultats électoraux pour passer d'une liste de candidats (Macron, Le Pen, etc.) à des **blocs politiques structurels** (Gauche, Centre, Droite, etc.). Pour ça, nous avons créé un jeu de données `mapping_politique` qui lie les partis politiques à des blocs politiques.

- **Robustesse 2027** : Le modèle apprend la signature politique d'un territoire. Il reste valide même si les noms des candidats changent en 2027.
- **Normalisation** : Toutes les cibles ($Y$) sont exprimées en **pourcentage des suffrages exprimés** pour éliminer le biais de taille des bureaux de vote.

### Nettoyage du Signal

- **Typologie des Bureaux** : Nous excluons systématiquement les bureaux de type **ORDINAIRE**. Seuls les bureaux de quartier classiques sont conservés.
- **Raison** : Les bureaux **ADMINISTRATIFS** regroupent des populations nomades (SDF, détenus, personnes du voyage) dont le comportement électoral est un artefact administratif décorrélé de la sociologie du quartier physique.

---

## 🛡️ Feature Engineering : Sécurité

Plutôt que d'injecter les 14 indicateurs bruts du Ministère de l'Intérieur au risque de créer du bruit statistique (sur-apprentissage), nous les avons regroupés en **3 Familles d'Insécurité** (Piliers).

_Note : Pour la justification théorique et sociologique détaillée de ce choix (Théorie de François Abel et étude du SSMSI), consultez l'onglet [Choix des Données ML](./ml_choix_donnees.md)._

### Dynamique Temporelle

Pour chaque pilier, nous calculons deux types de variables :

1. **Le Snapshot (2021)** : L'état des lieux immédiat pré-élection.
2. **Les Deltas (3 ans et 5 ans)** : L'évolution de la délinquance. Cela permet au modèle de déterminer si l'électeur vote en fonction du niveau absolu de crime ou de son **sentiment de dégradation** (Dynamique).

---

## 📊 Diagnostic de Performance (Baseline)

Suite à la construction de l'OBT, un premier benchmark a été réalisé pour établir une performance de référence (Baseline) sur la prédiction du **Bloc Gauche (Tour 1)**.

### Résultats du Benchmark Comparatif

| Modèle                  | Score $R^2$ | Erreur (RMSE) | Observation                                                   |
| :---------------------- | :---------: | :-----------: | :------------------------------------------------------------ |
| **Régression Linéaire** | **0.5013**  |     8.38%     | Explique 50% de la variance mais instable (Multicolinéarité). |
| **Random Forest**       | **0.3277**  |     9.73%     | Performance dégradée malgré la robustesse de l'algorithme.    |

### Analyse de l'Écart de Performance (Le "Plafond de Verre")

Le paradoxe d'un Random Forest moins performant qu'une simple régression linéaire s'explique par un **défaut de conception de l'OBT** (Phase 1) :

1. **Conflit de Granularité** : Les cibles ($Y$) sont au grain **Bureau de Vote** (304 unités), mais les caractéristiques sociales sont au grain **Arrondissement** (9 unités).
2. **Perte de Variance** : En attribuant la même valeur moyenne (revenu, pauvreté) à tous les bureaux d'un même arrondissement, on "gomme" la diversité sociologique. Un quartier aisé et un quartier populaire du même arrondissement deviennent identiques pour le modèle.
3. **Échec des Arbres de Décision** : Le Random Forest ne peut pas "découper" les données pour créer des règles précises si les features sont identiques pour des votes différents. Il finit par produire une moyenne floue, d'où un $R^2$ faible (0.33).

### Importance des Features (Insights)

Le Random Forest révèle tout de même quelles variables portent le plus de signal :

- **`feat_secu_violence_delta_5ans` (30%)** : La dynamique de la violence sur un mandat complet est le premier prédicteur.
- **`feat_participation_pct` (21%)** : La mobilisation électorale reste un pilier du score.
- **`feat_social_taux_pauvrete` (11%)** : Seul indicateur social émergeant à l'échelle de l'arrondissement.

---

## 🚀 Roadmap d'Amélioration (Soutenance)

Pour dépasser ce seuil de 50% d'explication et réduire l'erreur (RMSE) sous les 5 points, la stratégie technique retenue est la suivante :

1. **Refactorisation du Grain Social** : Abandonner l'agrégation par arrondissement au profit d'une **jointure spatiale par Carreaux Insee (200m)**. Cela permettra de donner une "identité sociale" unique à chaque bureau de vote.
2. **Gestion de la Multicolinéarité** : Utilisation de modèles de régularisation (**Lasso/Ridge**) pour stabiliser les coefficients et éliminer les variables redondantes (Deltas 3 ans vs 5 ans).
3. **Ingénierie de l'Interaction** : Tester des variables combinées (ex: Pauvreté $\times$ Sentiment d'insécurité) pour capturer des comportements non-linéaires.
