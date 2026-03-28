# Datai Forge - documentations et qualités

Ce portail centralise toute la documentation technique, les décisions d'architecture et les rapports de santé de notre modèle prédictif pour les élections présidentielles de 2027.

## 🎯 Objectif du Projet

Prédire les résultats du scrutin de 2027 à l'échelle de la ville de Lyon (Code INSEE 69123) en croisant des données historiques électorales, socio-économiques et de sécurité (Nous enrichirons les données avec d'autres indicateurs dans l'avenir)

## 🛠️ Stack Technique

Le projet repose sur certains standards industriels :

**Traitement** : Apache Spark 3.5 (PySpark)

- **Architecture** : Médaillon (Bronze, Silver, Gold)
- **Conteneurisation** : Docker & VS Code Dev Containers
- **CI/CD** : GitHub Actions
- **Qualité** : Pytest, Ruff, Bandit, MkDocs

## 🚀 Navigation Rapide

- **[Architecture](architecture.md)** : Comprendre comment nos données transitent entre les couches.
- **[Qualité & Santé](reports/coverage.md)** : Consulter les audits de sécurité et la couverture de tests en temps réel.

---

!!! info "Note de l'équipe"
Ce site est mis à jour automatiquement à chaque modification du code source sur la branche principale.
