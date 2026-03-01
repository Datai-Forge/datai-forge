import os
import logging
from datetime import datetime
from pyspark.sql.functions import lit

from src.config import RAW_DATA_PATH, PRESIDENTIELLE_BRONZE_PATH
from src.common.spark_session_manager import get_spark_session

# Config du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_and_load_bronze(spark, file_name):
    """
    On prend un fichier CSV brut, on lui ajoute du lignage et on le save en Parquet.
    """
    file_path = os.path.join(RAW_DATA_PATH, file_name)
    
    if not os.path.exists(file_path):
        logger.error(f"Fichier source introuvable : {file_path}")
        return

    # Pour le linéage en metadonnées
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_dir_name = file_name.replace('.csv', '')
    output_full_path = os.path.join(PRESIDENTIELLE_BRONZE_PATH, output_dir_name)

    logger.info(f"Début de l'ingestion Bronze pour : {file_name}")

    df = (spark.read
          .option("header", "true")
          .option("delimiter", ",")
          .option("inferSchema", "true")
          .csv(file_path))

    # Gouvernance de Données avec du lignage. Nom du fichier source + timestamp de traitement
    df = (df.withColumn("source_file", lit(file_name))
            .withColumn("processing_timestamp", lit(current_timestamp)))

    # On écrit en format Parquet (Format standard, compressé et ultra-rapide pour la suite car Databricks est optimisé pour ça)
    df.write.mode("overwrite").parquet(output_full_path)
    
    logger.info(f"Ingestion terminée avec succès dans : {output_full_path}")

    # Petit visuel rapide
    logger.info(f"Schéma détecté pour {file_name} :")
    df.printSchema()
    
    # Affichage des 5 premières lignes pour vérifier que tout est ok
    logger.info(f"Aperçu des 5 premières lignes :")
    df.show(5)

if __name__ == "__main__":
    # On démarre notre session Spark
    spark = get_spark_session(app_name="Ingestion_Bronze_Presidentielle")
    
    try:
        # Ingestion du tour 1 et 2
        extract_and_load_bronze(spark, "lyon_T1_presidentiel_2022.csv")
        extract_and_load_bronze(spark, "lyon_T2_presidentiel_2022.csv")
    finally:
        spark.stop()
        logger.info("Couche bronze terminée proprement.")
