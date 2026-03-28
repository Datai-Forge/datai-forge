# Infrastructure de Tests & Qualité (Lyon 2027)

Ce répertoire contient la suite de tests du projet.

## 🧪 Stratégie de Test

Le projet utilise une approche de validation à trois niveaux :

1. **Smoke Tests (Infrastructure)** : Vérifie que Spark, Java et Python sont correctement configurés dans le conteneur.
2. **Data Integrity Tests (Qualité)** : Valide les schémas, les checksums de votes et la cohérence géographique (Arrondissements de Lyon).
3. **Business Logic Tests (ETL)** : Assure que les transformations (Unpivot, Mapping politique) sont correctes.

## 🛠️ Outils utilisés

- **`pytest`** : Moteur de test principal.
- **`pytest-cov`** : Mesure de la couverture de tests.
- **`conftest.py`** : Centralise la `SparkSession` (fixture `spark`) pour une exécution ultra-rapide (session-scoped).
- **`reports/`** : Dossier généré contenant les rapports visuels de couverture.

## 🚀 Comment lancer les tests ?

Assurez-vous d'avoir lancé la pipeline de données au moins une fois (`scripts/run_etl_pipeline.sh`) avant de tester les couches Silver/Gold.

### 1. Lancement standard (avec couverture)

```bash
pytest
```

### 2. Audit de sécurité statique

```bash
bandit -c pyproject.toml -r .
```

### 3. Analyse de qualité de code (Linter)

```bash
ruff check .
```

## 📊 Lecture des rapports

Une fois `pytest` terminé, un rapport HTML interactif est disponible dans :
`reports/coverage/index.html`

Il vous permet de voir ligne par ligne quel code ETL a été exécuté durant la suite de tests.

---
