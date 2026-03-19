import os
import logging
from pyspark.sql.functions import lit, current_timestamp

from src.config import RAW_DATA_PATH, NIVEAU_VIE_PAUVRETE_BRONZE_PATH
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


    output_dir_name = file_name.replace('.csv', '')
    output_full_path = os.path.join(NIVEAU_VIE_PAUVRETE_BRONZE_PATH, output_dir_name)

    logger.info(f"Début de l'ingestion Bronze pour : {file_name}")

    # Réduit la pression mémoire sur un conteneur limité (~2 Go RAM)
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.conf.set("spark.default.parallelism", "2")

    df = (spark.read
          .option("header", "true")
          .option("delimiter", ",")
          .option("inferSchema", "false")
          .csv(file_path))

    # Gouvernance de Données avec du lignage. Nom du fichier source + timestamp de traitement
    df = (df.withColumn("source_file", lit(file_name))
            .withColumn("bronze_processing_timestamp", current_timestamp()))

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
    # chargement des datasets dans un tableau
    files = [
        "2017_Filosofi2017_carreaux_1km_met.csv",
        "2019_carreaux_1km_met.csv",
        "2021_carreaux_1km_met.csv",
    ]

    # pour chanque fichier, on démarre notre session Spark

    for file_name in files:
        #  On démarre notre session Spark
        spark = get_spark_session(app_name=f"Ingestion_Bronze_{file_name}")
        try:
            extract_and_load_bronze(spark, file_name)
        except Exception:
            logger.exception(f"Echec ingestion pour {file_name}")
        finally:
            try:
                spark.stop()
            except Exception:
                logger.warning(f"Impossible d'arrêter proprement Spark pour {file_name} (JVM déjà arrêtée).")
            logger.info(f"Session Spark fermée pour {file_name}")
