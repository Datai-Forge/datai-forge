import os
import logging
from pyspark.sql.functions import current_timestamp

from src.config import SILVER_PATH, PRESIDENTIELLE_BRONZE_PATH
from src.common.spark_session_manager import get_spark_session
from src.common.utils import clean_column_name

# Config du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PRESIDENTIELLE_SILVER_PATH = os.path.join(SILVER_PATH, "presidentielle")

def transform_bronze_to_silver(spark, folder_name):
    """
    Couche Silver :
    Lineage Silver
    Renommage métier via clean_column_name
    Drop des propriétés calculées
    """
    bronze_full_path = os.path.join(PRESIDENTIELLE_BRONZE_PATH, folder_name)
    silver_full_path = os.path.join(PRESIDENTIELLE_SILVER_PATH, folder_name)

    if not os.path.exists(bronze_full_path):
        logger.error(f"Dossier Bronze introuvable : {bronze_full_path}")
        return

    logger.info(f"Début de la transformation Silver pour : {folder_name}")

    df = spark.read.parquet(bronze_full_path)

    df = df.withColumn("silver_processing_timestamp", current_timestamp())

    # cible de drop = les colonnes calculées
    calculated_cols = [c for c in df.columns if "%" in c]
    df = df.drop(*calculated_cols)  # On drop directement les colonnes calculées avant renommage

    # renommage global via utils.clean_column_name
    new_column_names = [clean_column_name(c) for c in df.columns]
    df = df.toDF(*new_column_names)

    # Cast de certaines colonnes en string

    df = df.withColumn("code_du_departement", df["code_du_departement"].cast("string")) \
          .withColumn("code_de_la_circonscription", df["code_de_la_circonscription"].cast("string")) \
          .withColumn("code_de_la_commune", df["code_de_la_commune"].cast("string")) \
          .withColumn("code_du_b_vote", df["code_du_b_vote"].cast("string"))

    # écriture du df
    if not os.path.exists(PRESIDENTIELLE_SILVER_PATH):
        os.makedirs(PRESIDENTIELLE_SILVER_PATH, exist_ok=True)

    df.write.mode("overwrite").parquet(silver_full_path)

    logger.info(f"Transformation Silver terminée avec succès : {silver_full_path}")
    logger.info(f"Schéma Silver épuré pour {folder_name} :")
    df.printSchema()

if __name__ == "__main__":
    spark = get_spark_session(app_name="Silver_Presidentielle_Industrial")

    try:
        transform_bronze_to_silver(spark, "lyon_T1_presidentiel_2022")
        transform_bronze_to_silver(spark, "lyon_T2_presidentiel_2022")
    finally:
        spark.stop()
        logger.info("Pipeline Silver terminée proprement.")
