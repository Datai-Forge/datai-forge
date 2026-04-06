# 🧠 Justification du Choix des Données ML

Ce document détaille les fondements théoriques et sociologiques qui justifient l'utilisation des axes **Socio-Économique** (Revenus) et **Régalien** (Sécurité) pour la prédiction électorale de 2027.

## 🎓 Fondements Théoriques

Le choix de nos variables explicatives (_features_) repose sur les deux piliers majeurs de la science politique.

### 1. L'Axe Économique : La Théorie du "Vote de Portefeuille"

Selon les travaux de **François Abel** (Source : [Vie-Publique.fr](https://www.vie-publique.fr/parole-dexpert/299164-comment-leconomie-influence-t-elle-les-elections-francois-abel)), l'élection est souvent vécue comme un **référendum sur la gestion économique**.

- **L'Ignorance Rationnelle :** L'électeur utilise son niveau de vie immédiat et celui de son voisinage comme un raccourci cognitif pour juger le bilan du candidat sortant.
- **Application ML :** Nous utilisons les données **Insee Filosofi** (Revenu Médian, Taux de Pauvreté) pour capturer ce signal. Les évolutions récentes (Deltas) sont privilégiées car le vote se cristallise sur la situation des 6 derniers mois.

### 2. L'Axe Régalien : La Théorie du "Vote Culturel"

L'article souligne que lorsque la responsabilité économique semble diluée par la mondialisation, les électeurs se replient sur les compétences régaliennes de l'État : la **Protection** et l'**Ordre**.

- **Le Vote de Rupture :** L'insécurité (_réelle ou perçue_) devient le moteur principal des bascules électorales vers les blocs de droite radicale.
- **Application ML :** Nous utilisons les données du **SSMSI** regroupées en 3 familles (_Violence, Propriété, Rue/Stup_).

---

## 🛡️ Justification du Regroupement Sécurité

Pour garantir la performance du modèle, les 14 indicateurs bruts du SSMSI ont été regroupés. Ce choix est validé par le **Document de travail n°1 du SSMSI (2018)** ([Source](https://www.interieur.gouv.fr/Interstats/Publications-et-infographies/Documents-de-travail/Document-de-travail-n-1-Sentiment-d-insecurite-quelle-influence-de-la-delinquance-dans-le-voisinage)).

| Famille ML     | Justification Sociologique (SSMSI DT01)                           | Impact Électoral Attendu                  |
| :------------- | :---------------------------------------------------------------- | :---------------------------------------- |
| **Propriété**  | Augmente l'insécurité au **domicile** (vulnérabilité matérielle). | Vote de protection du patrimoine.         |
| **Rue & Stup** | Génère une forte **gêne** et dégrade le **cadre de vie**.         | Vote de rupture (demande d'ordre public). |
| **Violence**   | Signal de danger physique direct (espace public).                 | Sentiment de vulnérabilité physique.      |

---

## ⚙️ Intérêt Technique pour le Machine Learning

Le regroupement des données (Sociales et Sécurité) répond à trois impératifs :

1. **Réduction de la Dimensionnalité :** Éviter le "fléau de la dimension" en limitant le nombre de variables explicatives par rapport au nombre d'observations (608 bureaux de vote).
2. **Stabilité du Signal :** Transformer des micro-événements rares (ex: homicides) en scores continus robustes à l'échelle du bureau de vote.
3. **Généralisation :** Empêcher le modèle de mémoriser du "bruit" local pour se concentrer sur les tendances sociologiques lourdes.
