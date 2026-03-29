# Standards de Développement & Qualité

Ce document détaille les outils et processus automatisés du projet.

## 🛡️ Automatisation : Git Pre-commit

Le projet utilise des **hooks de pre-commit** pour valider chaque changement avant qu'il ne soit enregistré dans l'historique Git.

### Liste des vérifications

À chaque commit, les outils suivants sont exécutés :

1. **Nettoyage de base** :
   - `trailing-whitespace` : Supprime les espaces inutiles en fin de ligne.
   - `end-of-file-fixer` : Garantit que chaque fichier se termine par une seule ligne vide.
2. **Validation de configuration** :
   - `check-yaml` : Vérifie la syntaxe des fichiers `.yaml` (ex: CI/CD, config).
   - `check-added-large-files` : Empêche l'ajout accidentel de fichiers de données trop volumineux dans Git.
3. **Qualité du code Python (Ruff)** :
   - `ruff` : Analyse le code, trie les imports et applique des corrections automatiques (`--fix`).
   - `ruff-format` : Formate le code selon les standards PEP8 (longueur de ligne : 100).
4. **Hygiène des Notebooks** :
   - `nbstripout` : Supprime les sorties d'exécution et les métadonnées inutiles des fichiers `.ipynb` pour éviter de polluer les diffs Git.

### Commandes Utiles

- **Forcer un nettoyage complet** (sur tous les fichiers) :

  ```bash
  pre-commit run --all-files
  ```

- **Mettre à jour les outils** :

  ```bash
  pre-commit autoupdate
  ```

## 🧹 Audit de Qualité (Ruff)

Nous utilisons **Ruff** comme outil tout-en-un pour remplacer Flake8, isort et Black.

- **Vérification** : `ruff check .`
- **Formatage** : `ruff format .`

Les règles activées (E, F, W, B, I, UP) couvrent les erreurs de syntaxe, les variables inutilisées, le tri des imports.

## 🧪 Tests & Couverture (Pytest)

La validation fonctionnelle est assurée par **Pytest**.

- **Exécution** : `pytest`
- **Rapports** : Les rapports de couverture HTML sont générés dans `docs/coverage/` et sont intégrés au Hub de documentation.

## 🔒 Sécurité (Bandit)

L'outil **Bandit** scanne le code à la recherche de failles de sécurité courantes (utilisation de `eval`, mots de passe en dur, etc.).

- **Exécution** : `bandit -r src/`
