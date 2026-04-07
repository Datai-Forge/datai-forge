# 🛠️ Outils Complémentaires

En plus des outils standards, nous recommandons l'utilisation d'outils avancés pour optimiser votre flux de travail et garantir la robustesse de la CI/CD localement.

## 🎭 Act : Exécuter GitHub Actions en local

**Act** est un outil puissant qui permet de lancer vos workflows GitHub Actions directement sur votre machine. Il simule l'environnement de GitHub en utilisant des conteneurs Docker.

### 🌟 Pourquoi utiliser Act ?

- **Feedback Immédiat** : Plus besoin de `git push` et d'attendre 5 à 10 minutes pour savoir si votre modification de CI fonctionne.
- **Économie de ressources** : Vous n'utilisez pas vos minutes gratuites GitHub Actions pour des tests de configuration.
- **Sécurité** : Vous pouvez tester vos secrets et vos intégrations Docker localement avant de les publier.

### 📥 Installation

Selon votre système d'exploitation :

- **MacOS (Homebrew)** : `brew install act`
- **Linux** : `curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash`
- **Windows (Chocolatey)** : `choco install act-cli`

### 🚀 Utilisation dans le projet

Lors du premier job, le build la CI délenche un push vers le Docker Hub. Les informations sont récupérées via le fichier `/app/.secrets`.
Pour lancer les tests du projet comme si vous étiez sur GitHub :

Lister toutes les actions disponibles

```bash
act -l
```

Lancer uniquement le job de tests

```bash
act -j tests
```

Lancer tout le workflow (Build + Tests)

```bash
act
```

### 💡 Astuce en cas de problème

Si vous travaillez sur une architecture spécifique (ex: puce Apple M1/M2), utilisez l'option `--container-architecture linux/amd64` pour garantir la compatibilité avec les images Spark utilisées dans ce projet.

---

## 🧹 Pre-commit

Bien que configuré dans le projet, n'oubliez pas d'installer les hooks localement pour automatiser le linting avant chaque commit :

```bash
pre-commit install
```

Cela lancera **Ruff** et les audits de sécurité automatiquement à chaque fois que vous tenterez de valider votre code.
