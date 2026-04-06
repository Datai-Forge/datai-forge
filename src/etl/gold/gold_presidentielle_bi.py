import logging
import os

from pyspark.sql import functions as F

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH, SILVER_PATH

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_gold_bi_pipeline():
    """
    Transforme les données Silver en schéma en étoile pour la BI (Lyon).
    - dim_candidats : Référentiel des candidats
    - dim_geographie : Référentiel des bureaux de vote (avec type de bureau)
    - fact_votes : Nombre de voix par candidat et par tour
    - fact_participation : Inscrits, abstentions, votants, exprimés par tour
    """
    spark = get_spark_session("Gold_Presidentielle_BI")
    logger.info("Démarrage de la pipeline Gold BI...")

    # Chargement des données qui viennent de silver
    silver_t1_path = os.path.join(SILVER_PATH, "presidentielle", "lyon_T1_presidentiel_2022")
    silver_t2_path = os.path.join(SILVER_PATH, "presidentielle", "lyon_T2_presidentiel_2022")

    df_t1 = spark.read.parquet(silver_t1_path).withColumn("tour", F.lit(1))
    df_t2 = spark.read.parquet(silver_t2_path).withColumn("tour", F.lit(2))

    # Union des deux tours
    df_silver = df_t1.unionByName(df_t2)

    # Dimension candidat
    logger.info("Extraction de dim_candidats...")
    dim_candidats = (
        df_silver.select(
            "id_candidat",
            "nom",
            "prenom",
            "sexe",
            "parti_code",
            "parti_nom",
            "nuance_officielle",
            "bloc_analytique",
        )
        .distinct()
        .filter(F.col("id_candidat").isNotNull())
    )

    # Dimension géographique
    logger.info("Extraction de dim_geographie...")
    # Règle métier : Identifier le bureau 0001 comme Rattachement Administratif
    dim_geographie = (
        df_silver.select(
            F.col("code_du_b_vote").alias("id_bureau"), "libelle_de_la_commune", "arrondissement"
        )
        .distinct()
        .withColumn("code_insee", F.lit("69123"))
        .withColumn(
            "type_bureau",
            F.when(F.col("id_bureau") == "0001", "Rattachement Administratif").otherwise(
                "Standard"
            ),
        )
    )

    # Table de faits pour les votes
    logger.info("Extraction de fact_votes...")
    fact_votes = df_silver.select(
        F.col("code_du_b_vote").alias("id_bureau"), "id_candidat", "tour", "voix"
    ).filter(F.col("id_candidat").isNotNull())

    # Table de faits pour la participation
    logger.info("Extraction de fact_participation...")
    # On prend une seule ligne par bureau et par tour
    fact_participation = df_silver.select(
        F.col("code_du_b_vote").alias("id_bureau"),
        "tour",
        "inscrits",
        "abstentions",
        "votants",
        "exprimes",
    ).distinct()

    # Ajout d'indicateurs pré-calculés
    fact_participation = fact_participation.withColumn(
        "taux_participation", F.round((F.col("votants") / F.col("inscrits")) * 100, 2)
    ).withColumn("taux_abstention", F.round((F.col("abstentions") / F.col("inscrits")) * 100, 2))

    # pour la sauvegarde
    output_base_path = os.path.join(GOLD_PATH, "presidentielle", "bi")
    logger.info(f"Sauvegarde des tables Gold BI dans {output_base_path}...")

    # On utilise des sous-dossiers explicites
    dim_candidats.write.mode("overwrite").parquet(os.path.join(output_base_path, "dim_candidats"))
    dim_geographie.write.mode("overwrite").parquet(os.path.join(output_base_path, "dim_geographie"))
    fact_votes.write.mode("overwrite").parquet(os.path.join(output_base_path, "fact_votes"))
    fact_participation.write.mode("overwrite").parquet(
        os.path.join(output_base_path, "fact_participation")
    )

    # Audit rapide
    total_voix = fact_votes.agg(F.sum("voix")).collect()[0][0]
    total_exprimes = fact_participation.agg(F.sum("exprimes")).collect()[0][0]

    logger.info(f"Audit Gold BI : Total Voix={total_voix}, Total Exprimés={total_exprimes}")
    if total_voix == total_exprimes:
        logger.info("✅ Intégrité des données confirmée.")
    else:
        logger.warning("⚠️ Écart détecté entre les voix et les exprimés !")

    logger.info("Pipeline Gold BI terminée avec succès !")


if __name__ == "__main__":
    run_gold_bi_pipeline()
