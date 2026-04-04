import logging
import os

from pyspark.sql import functions as F

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH, SILVER_PATH

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_gold_ml_step1_base_analytical():
    """
    Étape 1 : Préparer la base électorale par Blocs Analytiques.
    - Unité : Bureau de Vote x Tour
    - Cibles (Y) : target_score_[BLOC]_pct
    - Features (X) : feat_abstention_pct, feat_participation_pct
    """
    spark = get_spark_session("Gold_ML_Step1_Analytical")
    logger.info("Démarrage de l'étape 1 : Préparation de la base par Blocs Politiques...")

    # Chargement des données Silver
    t1_path = os.path.join(SILVER_PATH, "presidentielle", "lyon_T1_presidentiel_2022")
    t2_path = os.path.join(SILVER_PATH, "presidentielle", "lyon_T2_presidentiel_2022")

    df_t1 = spark.read.parquet(t1_path).withColumn("tour", F.lit(1))
    df_t2 = spark.read.parquet(t2_path).withColumn("tour", F.lit(2))
    df_votes = df_t1.unionByName(df_t2)

    # On exclut les bureaux administratifs (ex: 0001) pour le ML
    logger.info("Filtrage des bureaux (on ne garde que le type ORDINAIRE)...")
    df_votes = df_votes.filter(F.col("type_bureau") == "ORDINAIRE")

    # Agrégation par Bloc Analytique avant Pivot
    # (Important : plusieurs candidats peuvent appartenir au même bloc au T1)
    logger.info("Agrégation des voix par bloc analytique...")
    df_grouped = df_votes.groupBy(
        "code_du_b_vote",
        "tour",
        "code_insee_arrondissement",
        "inscrits",
        "exprimes",
        "abstentions",
        "bloc_analytique",
    ).agg(F.sum("voix").alias("total_voix_bloc"))

    # Pivotement des Blocs (Long -> Wide)
    logger.info("Pivotement des blocs (format Wide)...")
    df_pivot = (
        df_grouped.groupBy(
            "code_du_b_vote",
            "tour",
            "code_insee_arrondissement",
            "inscrits",
            "exprimes",
            "abstentions",
        )
        .pivot("bloc_analytique")
        .agg(F.sum("total_voix_bloc"))
    )

    # Calcul des pourcentages (%) pour les cibles (Y)
    # On identifie les colonnes de blocs (toutes sauf les métadonnées)
    meta_cols = [
        "code_du_b_vote",
        "tour",
        "code_insee_arrondissement",
        "inscrits",
        "exprimes",
        "abstentions",
    ]
    bloc_cols = [c for c in df_pivot.columns if c not in meta_cols]

    df_base = df_pivot
    for bloc in bloc_cols:
        col_name = f"target_score_{bloc.lower()}_pct"
        df_base = df_base.withColumn(
            col_name,
            F.when(
                F.col("exprimes") > 0,
                F.round((F.coalesce(F.col(bloc), F.lit(0)) / F.col("exprimes")) * 100, 2),
            ).otherwise(0.0),
        )
        df_base = df_base.drop(bloc)

    # Calcul des Features électorales (X)
    df_base = df_base.withColumn(
        "feat_abstention_pct",
        F.when(
            F.col("inscrits") > 0, F.round((F.col("abstentions") / F.col("inscrits")) * 100, 2)
        ).otherwise(0.0),
    )

    df_base = df_base.withColumn(
        "feat_participation_pct", F.round(100 - F.col("feat_abstention_pct"), 2)
    )

    # Sélection finale et nettoyage
    final_cols = [
        F.col("code_du_b_vote").alias("id_bureau"),
        "tour",
        "code_insee_arrondissement",
        "feat_abstention_pct",
        "feat_participation_pct",
    ] + [c for c in df_base.columns if c.startswith("target_score_")]

    df_final = df_base.select(*final_cols).fillna(0.0)

    # Sauvegarde
    output_path = os.path.join(GOLD_PATH, "ml", "base_elections_analytical")
    logger.info(f"Sauvegarde du socle électoral dans {output_path}...")
    df_final.write.mode("overwrite").parquet(output_path)

    # Audit rapide
    n_rows = df_final.count()
    logger.info(f"Étape 1 terminée : {n_rows} lignes générées.")
    df_final.show(5)


if __name__ == "__main__":
    run_gold_ml_step1_base_analytical()
