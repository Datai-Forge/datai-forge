# Audit de Sécurité (Bandit)

Le code Python est scanné à chaque exécution pour détecter d'éventuelles failles de sécurité.

## 🛡️ Résultats du dernier scan

Le rapport détaillé est généré au format texte.

[Consulter le rapport complet (TXT)](security-report.txt)

## 🔍 Ce que nous vérifions

- Utilisation de fonctions dangereuses (`eval()`, `exec()`).
- Secrets ou mots de passe en dur dans le code.
- Permissions de fichiers non sécurisées.
- Configuration de Spark et des connexions réseaux.
