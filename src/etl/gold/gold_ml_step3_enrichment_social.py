import logging
import os

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH, SILVER_PATH

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_gold_ml_step3_enrichment_social():
    """
    Étape 3 : Enrichissement final de l'OBT avec les données Socio-Économiques.
    - Agrégation massive des données Insee Silver par Arrondissement.
    - Calcul des features sociales (Ratios et Deltas).
    - Jointure finale pour obtenir l'OBT ML complète.
    """
    spark = get_spark_session("Gold_ML_Step3_Social")
    logger.info("Démarrage de l'étape 3 : Enrichissement Socio-Économique...")

    # Chargement de l'OBT existante (Elections + Sécurité)
    obt_step2_path = os.path.join(GOLD_PATH, "ml", "obt_step2_security")
    df_obt = spark.read.parquet(obt_step2_path)

    # Chargement et Fusion des données Silver Social (2017, 2019, 2021)
    base_social_path = os.path.join(SILVER_PATH, "niveau_vie_pauvrete")

    dfs_social = []
    for year in [2017, 2019, 2021]:
        path = os.path.join(base_social_path, f"niveau_vie_pauvrete_{year}")
        if not os.path.exists(path):
            logger.warning(f"Dossier {path} introuvable, skip.")
            continue
        df_year = spark.read.parquet(path).withColumn("annee", F.lit(year))

        # On utilise la colonne 'arrondissement' (ex: 69381) pour la jointure
        df_year = df_year.withColumn("id_arr_social", F.col("arrondissement"))
        dfs_social.append(df_year)

    if not dfs_social:
        logger.error("Aucune donnée sociale trouvée !")
        return

    df_social_all = dfs_social[0]
    for df in dfs_social[1:]:
        df_social_all = df_social_all.unionByName(df, allowMissingColumns=True)

    # agrégation par Arrondissement et par Année
    numeric_types = (FloatType, DoubleType, LongType, IntegerType)
    cols_to_sum = [
        field.name
        for field in df_social_all.schema.fields
        if isinstance(field.dataType, numeric_types)
        and field.name not in ["annee", "id_arr_social"]
    ]

    logger.info(f"Agrégation de {len(cols_to_sum)} indicateurs sociaux...")
    df_agg = df_social_all.groupBy("id_arr_social", "annee").agg(
        *[F.sum(c).alias(c) for c in cols_to_sum]
    )

    # Feature Engineering : Calcul des Ratios par Arrondissement
    df_features = (
        df_agg.withColumn(
            "feat_social_taux_pauvrete",
            F.round((F.col("nb_menages_pauvres") / F.col("nb_menages")) * 100, 2),
        )
        .withColumn(
            "feat_social_revenu_moyen",
            F.round(
                F.col("somme_niveaux_de_vie_winsorises_des_individus") / F.col("nb_individus"), 2
            ),
        )
        .withColumn(
            "feat_social_pct_proprietaires",
            F.round((F.col("nb_menages_propriétaires") / F.col("nb_menages")) * 100, 2),
        )
        .withColumn(
            "feat_social_pct_logements_sociaux",
            F.round((F.col("nb_logements_sociaux") / F.col("nb_menages")) * 100, 2),
        )
    )

    # Calcul des Deltas (2021 vs 2017)
    df_2017 = df_features.filter(F.col("annee") == 2017).select(
        F.col("id_arr_social"),
        F.col("feat_social_revenu_moyen").alias("rev17"),
        F.col("feat_social_taux_pauvrete").alias("pauv17"),
    )

    df_2021 = df_features.filter(F.col("annee") == 2021).select(
        "id_arr_social",
        "feat_social_revenu_moyen",
        "feat_social_taux_pauvrete",
        "feat_social_pct_proprietaires",
        "feat_social_pct_logements_sociaux",
    )

    df_social_final = (
        df_2021.join(df_2017, "id_arr_social", "inner")
        .withColumn(
            "feat_social_delta_revenu_5ans",
            F.round(
                ((F.col("feat_social_revenu_moyen") - F.col("rev17")) / F.col("rev17")) * 100, 2
            ),
        )
        .withColumn(
            "feat_social_delta_pauvrete_5ans",
            F.round(
                ((F.col("feat_social_taux_pauvrete") - F.col("pauv17")) / F.col("pauv17")) * 100, 2
            ),
        )
        .select(
            "id_arr_social",
            "feat_social_revenu_moyen",
            "feat_social_taux_pauvrete",
            "feat_social_pct_proprietaires",
            "feat_social_pct_logements_sociaux",
            "feat_social_delta_revenu_5ans",
            "feat_social_delta_pauvrete_5ans",
        )
    )

    # Jointure Finale avec l'OBT
    logger.info("Fusion finale de toutes les dimensions dans l'OBT...")
    # On caste les deux clés en string pour être sûr
    df_obt = df_obt.withColumn("join_arr", F.col("code_insee_arrondissement").cast("string"))
    df_social_final = df_social_final.withColumn(
        "join_arr_social", F.col("id_arr_social").cast("string")
    )

    df_final = df_obt.join(
        df_social_final, df_obt.join_arr == df_social_final.join_arr_social, "left"
    ).drop("join_arr", "join_arr_social", "id_arr_social")

    # Sauvegarde de la One Big Table (OBT) Finale
    output_path = os.path.join(GOLD_PATH, "ml", "obt_ml_complete")
    logger.info(f"Sauvegarde de l'OBT COMPLETE dans {output_path}...")
    df_final.write.mode("overwrite").parquet(output_path)

    # Audit de fin
    logger.info(f"Pipeline terminée. Grain final : {df_final.count()} lignes (608 attendues).")
    logger.info(f"Nombre de features totales : {len(df_final.columns)}")
    df_final.show(5)


if __name__ == "__main__":
    run_gold_ml_step3_enrichment_social()
