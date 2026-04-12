import logging
import os

from pyspark.sql.functions import coalesce, col, concat, current_timestamp, lit, to_date, year
from pyspark.sql.types import DoubleType, IntegerType

from src.common.spark_session_manager import get_spark_session
from src.config import SECURITE_BRONZE_PATH, SILVER_PATH

# Config du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SECURITE_SILVER_PATH = os.path.join(SILVER_PATH, "securite")


def transform_bronze_to_silver(spark):
    """
    Transforme les données de sécurité de la couche Bronze vers Silver.
    Application du filtrage Lyon et du typage des colonnes.
    """
    bronze_full_path = os.path.join(SECURITE_BRONZE_PATH, "security-filtered")
    silver_full_path = os.path.join(SECURITE_SILVER_PATH, "lyon_securite")

    if not os.path.exists(bronze_full_path):
        logger.error(f"Dossier Bronze introuvable : {bronze_full_path}")
        return

    logger.info("Début de la transformation Silver pour la sécurité (Lyon)")

    df = spark.read.parquet(bronze_full_path)

    # 1. Sélection, Renommage et Conversion en Date (01/01/année)
    # 2. Casting des types numériques et gestion des estimations
    df_silver = df.select(
        to_date(concat(col("annee"), lit("-01-01")), "yyyy-MM-dd").alias("date_reference"),
        col("CODGEO_2025").alias("code_geo"),
        col("indicateur"),
        col("unite_de_compte"),
        # Règle Métier : Si le nombre est NULL (ndiff), on prend l'estimation du complément
        coalesce(
            col("nombre").cast(IntegerType()), col("complement_info_nombre").cast(IntegerType())
        ).alias("nombre"),
        # Règle Métier : Idem pour le taux (redressement statistique)
        coalesce(
            col("taux_pour_mille").cast(DoubleType()),
            col("complement_info_taux").cast(DoubleType()),
        ).alias("taux_pour_1000"),
        col("insee_pop").cast(IntegerType()).alias("population"),
        col("insee_log").cast(IntegerType()).alias("logements"),
        # On garde quand même les colonnes d'estimation pour le ML (sans NULL)
        coalesce(col("complement_info_nombre").cast(DoubleType()), lit(0.0)).alias("nombre_estime"),
        coalesce(col("complement_info_taux").cast(DoubleType()), lit(0.0)).alias("taux_estime"),
        col("source_file"),
        col("bronze_processing_timestamp"),
    ).withColumn("silver_processing_timestamp", current_timestamp())

    # 3. Filtrage : Uniquement Lyon ET Plage temporelle complète (2017 à 2022 inclus)
    df_silver = df_silver.filter(
        (col("code_geo").rlike("^6938[1-9]$")) & (year(col("date_reference")).between(2017, 2022))
    )

    # Sauvegarde
    if not os.path.exists(SECURITE_SILVER_PATH):
        os.makedirs(SECURITE_SILVER_PATH, exist_ok=True)

    df_silver.write.mode("overwrite").parquet(silver_full_path)

    logger.info(f"Transformation Silver terminée : {silver_full_path}")
    logger.info(f"Nombre de lignes finales : {df_silver.count()}")
    df_silver.printSchema()


if __name__ == "__main__":
    spark = get_spark_session(app_name="Silver_Securite_Lyon")

    try:
        transform_bronze_to_silver(spark)
    finally:
        spark.stop()
        logger.info("Pipeline Silver Securite terminée.")
