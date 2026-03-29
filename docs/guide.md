# Guide du Développeur

Ce guide détaille les procédures pour contribuer à Datai Forge.

## ⚙️ Flux de Travail

### 1. Développement de Scripts ETL

Les scripts de transformation doivent être placés dans `src/etl/`. Pour lancer un script en respectant les imports de modules :

```bash
python -m src.etl.bronze.bronze_presidentielle
```

### 🚀 Lancement Global de la Pipeline (Bash)

Le projet dispose d'un script d'automatisation pour exécuter l'intégralité du flux ETL (de l'ingestion Bronze à la modélisation Gold) :

```bash
chmod +x scripts/run_etl_pipeline.sh
./scripts/run_etl_pipeline.sh
```

Ce script est utile pour valider la chaîne de données de bout en bout.

### 2. Exploration Interactive

Utilisez le dossier `notebooks/` pour vos analyses exploratoires. L'extension Jupyter de VS Code est pré-configurée pour utiliser le kernel Spark du conteneur.

### 3. Débogage

Ouvrez n'importe quel script Python et appuyez sur **F5**. La configuration `.vscode/launch.json` a été implémentée pour lancer le script courant avec le debugger interactif.

## 💡 Standards & Bonnes Pratiques

- **Centralisation des Chemins** : Utilisez exclusivement `src.config` pour accéder aux dossiers de données. Aucun chemin relatif ne doit être "hardcodé".
- **Gestion de la Session Spark** : Importez `get_spark_session` depuis `src.common.spark_session_manager`.
- **Lignage des Données** : Chaque table générée doit inclure les colonnes de métadonnées `source_file` et `processing_timestamp`.
- **Format de Stockage** : Le format **Parquet** est obligatoire entre les couches (compatibilité avec Databricks).
- **Qualité & CI/CD** : Consultez la page [Standards & Qualité](standards.md) pour connaître les outils d'audit et de pre-commit.
