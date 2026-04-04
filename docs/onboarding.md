# 🚀 Onboarding

Bienvenue sur le projet **Datai Forge**. Ce guide a pour but de vous aider à configurer votre environnement et à lancer le projet rapidement en utilisant les standards de l'industrie.

## 🛠️ Configuration de l'Environnement

Le projet est entièrement conteneurisé pour uniformiser les environnements et faciliter la collaboration.

### 1. Pré-requis

- **Docker Desktop** installé et fonctionnel.
- **VS Code** installé.
- Extension VS Code **"Dev Containers"** (ms-vscode-remote.remote-containers) installée.

### 2. Lancement via VS Code (Recommandé)

C'est la méthode la plus simple pour profiter du kernel Spark et du debugger intégré.

1. Ouvrez le dossier du projet dans VS Code.
2. Une notification devrait apparaître en bas à droite : _"Folder contains a Dev Container configuration file. Reopen to folder to develop in a container"_.
3. Cliquez sur **"Reopen in Container"**.
4. VS Code va builder l'image Docker (la première fois uniquement) et monter votre code à l'intérieur.
5. Une fois terminé, vous êtes dans un environnement Linux complet avec toutes les dépendances installées.

### 3. Lancement manuel via Docker

Si vous préférez utiliser le terminal classique :

```bash
# Build de l'image
docker build -t datai-forge .
```

### Lancement d'un conteneur interactif

```bash
docker run -it --rm -v $(pwd):/app datai-forge bash
```

## ⚙️ Flux de Travail

### 🚀 Lancement Global de la Pipeline

Pour exécuter l'intégralité du flux de données (Bronze ➔ Silver ➔ Gold) :

```bash
chmod +x scripts/run_etl_pipeline.sh
./scripts/run_etl_pipeline.sh
```

### 🐍 Exécution d'un script spécifique

Pour lancer un script de transformation en respectant les imports :

```bash
python3 -m src.etl.bronze.bronze_presidentielle
```

### 📓 Exploration via Notebooks

Les analyses se font dans le dossier `notebooks/`.

- Ouvrez un fichier `.ipynb`.
- Sélectionnez le Kernel **"Python 3.12 (inside Dev Container)"**.
- Spark est pré-configuré pour fonctionner localement dans le notebook.

## 💡 Standards de Développement

- **Centralisation des Chemins** : Utilisez exclusivement `src.config`.
- **Lignage des Données** : Chaque table doit inclure `source_file` et `processing_timestamp`.
- **Format Parquet** : Format obligatoire pour la persistance entre les couches (optimisation Spark/Databricks).
- **Qualité** : Avant de commit, lancez `ruff check .` pour valider le style.
