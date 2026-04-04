# 🗳️ Dictionnaire des Données : Élections (Source Originale)

Ce document décrit la sémantique de chaque colonne présente dans le jeu de données original des élections présidentielles 2022 (Source : Ministère de l'Intérieur / data.gouv.fr).

## 📍 Localisation Administrative

| Colonne                         | Description                                                                                                                                                            |
| :------------------------------ | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Code du département`           | Code Insee du département (ex: 69 pour le Rhône).                                                                                                                      |
| `Libellé du département`        | Nom complet du département.                                                                                                                                            |
| `Code de la circonscription`    | Identifiant numérique de la circonscription législative.                                                                                                               |
| `Libellé de la circonscription` | Nom de la circonscription législative.                                                                                                                                 |
| `Code de la commune`            | Code Insee de la commune (ex: 69123 pour Lyon).                                                                                                                        |
| `Libellé de la commune`         | Nom de la commune.                                                                                                                                                     |
| `Code du b.vote`                | Identifiant unique du bureau de vote au sein de la commune. Ce code est composé de l'arrondissement sur deux chiffres en préfixe et du N° du bureau de vote en suffixe |

## 📊 Statistiques de Participation (Grain : Bureau de Vote)

| Colonne        | Description                                                                           |
| :------------- | :------------------------------------------------------------------------------------ |
| `Inscrits`     | Nombre total d'électeurs inscrits sur les listes électorales du bureau.               |
| `Abstentions`  | Nombre d'inscrits n'ayant pas participé au scrutin.                                   |
| `% Abs/Ins`    | Pourcentage d'abstention par rapport au nombre d'inscrits.                            |
| `Votants`      | Nombre d'inscrits ayant déposé un bulletin dans l'urne.                               |
| `% Vot/Ins`    | Pourcentage de participation (Taux de participation).                                 |
| `Blancs`       | Nombre de bulletins blancs déposés.                                                   |
| `% Blancs/Ins` | Part des bulletins blancs parmi les inscrits.                                         |
| `% Blancs/Vot` | Part des bulletins blancs parmi les votants.                                          |
| `Nuls`         | Nombre de bulletins nuls.                                                             |
| `% Nuls/Ins`   | Part des bulletins nuls parmi les inscrits.                                           |
| `% Nuls/Vot`   | Part des bulletins nuls parmi les votants.                                            |
| `Exprimés`     | Nombre de suffrages comptabilisés pour les candidats (`Votants` - `Blancs` - `Nuls`). |
| `% Exp/Ins`    | Part des suffrages exprimés parmi les inscrits.                                       |
| `% Exp/Vot`    | Part des suffrages exprimés parmi les votants.                                        |

## 👤 Détail des Suffrages par Candidat (Grain : Candidat)

| Colonne      | Description                                                                                                                                                                                                                                              |
| :----------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `N°Panneau`  | Numéro d'ordre officiel du candidat pour l'affichage électoral. Plus d'info par ici [Attribution panneaux d'affichage](https://presidentielle2022.conseil-constitutionnel.fr/l-election/la-campagne/quelles-sont-les-regles-concernant-l-affichage.html) |
| `Sexe`       | Sexe du candidat.                                                                                                                                                                                                                                        |
| `Nom`        | Nom de famille du candidat.                                                                                                                                                                                                                              |
| `Prénom`     | Prénom du candidat.                                                                                                                                                                                                                                      |
| `Voix`       | Nombre total de suffrages obtenus par ce candidat dans le bureau de vote.                                                                                                                                                                                |
| `% Voix/Ins` | Part des voix du candidat par rapport au nombre d'inscrits.                                                                                                                                                                                              |
| `% Voix/Exp` | Part des voix du candidat par rapport au total des suffrages exprimés.                                                                                                                                                                                   |

---

## 💡 Notes Techniques

- **Format** : Les fichiers originaux sont fournis au format CSV.
- **Granularité** : Une ligne par bureau de vote et par candidat (format "Long").
- **Unicité** : La combinaison de `Code de la commune`, `Code du b.vote` et `Nom` constitue une clé unique par tour.
