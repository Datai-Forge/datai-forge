# Guide du Développeur

Ce guide détaille les standards de l'équipe et les procédures pour contribuer aux pipelines ETL du projet Lyon 2027.

## ⚙️ Flux de Travail

### 1. Développement de Scripts ETL

Les scripts de transformation doivent être placés dans `src/etl/`. Pour lancer un script en respectant les imports de modules :

```bash
python -m src.etl.bronze.bronze_presidentielle
```

### 2. Exploration Interactive

Utilisez le dossier `notebooks/` pour vos analyses exploratoires. L'extension Jupyter de VS Code est pré-configurée pour utiliser le kernel Spark du conteneur.

### 3. Débogage

Ouvrez n'importe quel script Python et appuyez sur **F5**. La configuration `.vscode/launch.json` est optimisée pour lancer le script courant avec le debugger interactif.

## 💡 Standards & Bonnes Pratiques

- **Centralisation des Chemins** : Utilisez exclusivement `src.config` pour accéder aux dossiers de données. Aucun chemin relatif ne doit être "hardcodé".
- **Gestion de la Session Spark** : Importez `get_spark_session` depuis `src.common.spark_session_manager`.
- **Lignage des Données** : Chaque table générée doit inclure les colonnes de métadonnées `source_file` et `processing_timestamp`.
- **Format de Stockage** : Le format **Parquet** est obligatoire entre les couches pour garantir performance et compatibilité avec Databricks.
- **Qualité** : Avant chaque commit, assurez-vous de passer les tests (`pytest`) et le linter (`ruff check .`).
