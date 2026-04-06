# 📖 Référentiel des Variables (Features ML)

Ce document sert de dictionnaire technique pour les variables d'entrée (Features) utilisées dans nos modèles de prédiction électorale. Chaque variable a été sélectionnée pour son **poids sociologique** et sa capacité à expliquer un comportement de vote.

---

## 🏗️ Pilier 1 : Socio-Économique (Source : Insee)

_Ces variables décrivent le profil de vie et la structure sociale des habitants du quartier._

| Variable technique                  | Description "Métier"                   | Signification sociologique                                          |
| :---------------------------------- | :------------------------------------- | :------------------------------------------------------------------ |
| `feat_social_revenu_moyen`          | Revenu moyen par habitant              | Mesure le niveau de richesse et le pouvoir d'achat du quartier.     |
| `feat_social_taux_pauvrete`         | % de ménages sous le seuil de pauvreté | Indice de précarité économique et d'exclusion sociale.              |
| `feat_social_pct_proprietaires`     | % de ménages propriétaires             | Indicateur de stabilité résidentielle et d'ancrage territorial.     |
| `feat_social_pct_logements_sociaux` | % de logements HLM                     | Mesure la mixité sociale et l'influence des politiques de logement. |
| `feat_social_delta_revenu_5ans`     | Évolution du revenu (Mandat)           | Mesure le sentiment d'ascension ou de déclassement social.          |
| `feat_social_delta_pauvrete_5ans`   | Évolution de la pauvreté (Mandat)      | Capte la dynamique de paupérisation d'un quartier sur 5 ans.        |

---

## 🛡️ Pilier 2 : Sécurité et Délinquance (Source : SSMSI)

_Ces variables mesurent l'environnement de sécurité et le "sentiment d'insécurité" local._

| Variable technique               | Description "Métier"                     | Signification sociologique                                                   |
| :------------------------------- | :--------------------------------------- | :--------------------------------------------------------------------------- |
| `feat_secu_violence_2021`        | Atteintes volontaires aux personnes      | Mesure le niveau de violence physique et d'insécurité directe.               |
| `feat_secu_propriete_2021`       | Atteintes aux biens (vols, cambriolages) | Indice de la criminalité liée au profit et à la dégradation du cadre de vie. |
| `feat_secu_rue_stup_2021`        | Trafics de rue et stupéfiants            | Capte les incivilités de proximité et les réseaux de rue.                    |
| `feat_secu_violence_delta_5ans`  | Dynamique de la violence (5 ans)         | Évalue si la sécurité des personnes s'est dégradée durant le mandat.         |
| `feat_secu_propriete_delta_5ans` | Dynamique des vols (5 ans)               | Mesure le sentiment de "perte de contrôle" sur les biens privés.             |
| `feat_secu_rue_stup_delta_3ans`  | Dynamique des trafics (Post-COVID)       | Analyse l'explosion ou la réduction des trafics de rue récents.              |

---

## 🗳️ Pilier 3 : Comportement Électoral (Source : Min. Intérieur)

_Ces variables décrivent l'engagement civique et la mobilisation du territoire._

| Variable technique       | Description "Métier"      | Signification sociologique                                       |
| :----------------------- | :------------------------ | :--------------------------------------------------------------- |
| `feat_abstention_pct`    | Taux d'abstention (%)     | Mesure le désengagement, la protestation ou l'apathie politique. |
| `feat_participation_pct` | Taux de participation (%) | Indice de mobilisation citoyenne et de vitalité démocratique.    |

---

## 💡 Note sur la Standardisation

Pour notre modèle de **Régression Linéaire**, toutes ces variables sont systématiquement **centrées et réduites** (StandardScaler). Cela permet au modèle de comparer l'influence d'un euro de revenu supplémentaire avec celle d'un point de délinquance sur la même échelle de poids.

> _Pour plus d'informations sur l'influence réelle de ces variables, consultez le [Résumé Exécutif de la Modélisation](../resume_modelisation.md)._
