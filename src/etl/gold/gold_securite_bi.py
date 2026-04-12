import logging
import os

from pyspark.sql import functions as F

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH, SILVER_PATH

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_gold_securite_bi_pipeline():
    """
    Transforme les données Silver de sécurité en schéma en étoile pour la BI (Lyon).
    - dim_indicateurs_securite : Référentiel des types de crimes (15 indicateurs)
    - dim_geographie_lyon : Référentiel des arrondissements (69381-69389)
    - fact_securite : Métriques de délinquance par année/arrondissement
    - fact_demographie_annuelle : Population et logements par année/arrondissement
      (évite le double-comptage)
    """
    spark = get_spark_session("Gold_Securite_BI")
    logger.info("Démarrage de la pipeline Gold Securite BI...")

    # Chargement des données Silver
    silver_securite_path = os.path.join(SILVER_PATH, "securite", "lyon_securite")

    if not os.path.exists(silver_securite_path):
        logger.error(f"Données Silver introuvables : {silver_securite_path}")
        return

    df_silver = spark.read.parquet(silver_securite_path)

    # Ajout de l'année pour simplifier les tables de faits
    df_silver = df_silver.withColumn("annee", F.year("date_reference"))

    # Dimension Indicateurs (Les 15 types de crimes)
    logger.info("Extraction de dim_indicateurs_securite...")
    dim_indicateurs = df_silver.select(
        F.col("indicateur").alias("id_indicateur"), "unite_de_compte"
    ).distinct()

    # Dimension Géographie (Arrondissements de Lyon)
    logger.info("Extraction de dim_geographie_lyon...")
    dim_geographie = (
        df_silver.select(F.col("code_geo").alias("code_arrondissement"))
        .distinct()
        .withColumn(
            "nom_arrondissement",
            F.when(F.col("code_arrondissement") == "69381", "1er Arrondissement")
            .when(F.col("code_arrondissement") == "69382", "2ème Arrondissement")
            .when(F.col("code_arrondissement") == "69383", "3ème Arrondissement")
            .when(F.col("code_arrondissement") == "69384", "4ème Arrondissement")
            .when(F.col("code_arrondissement") == "69385", "5ème Arrondissement")
            .when(F.col("code_arrondissement") == "69386", "6ème Arrondissement")
            .when(F.col("code_arrondissement") == "69387", "7ème Arrondissement")
            .when(F.col("code_arrondissement") == "69388", "8ème Arrondissement")
            .when(F.col("code_arrondissement") == "69389", "9ème Arrondissement")
            .otherwise("Inconnu"),
        )
    )

    ## Deux tables: on évite de sommer la population 15x par indicateur en BI:
    # Table de faits : Sécurité (Nombre et Taux)
    logger.info("Extraction de fact_securite...")
    fact_securite = df_silver.select(
        F.col("code_geo").alias("code_arrondissement"),
        F.col("indicateur").alias("id_indicateur"),
        "annee",
        "nombre",
        "taux_pour_1000",
    )

    # Table de faits : Démographie (Socio-éco contextuel)
    # Règle Métier : On isole la population pour éviter de la sommer 15 fois en BI
    logger.info("Extraction de fact_demographie_annuelle...")
    fact_demographie = df_silver.select(
        F.col("code_geo").alias("code_arrondissement"), "annee", "population", "logements"
    ).distinct()

    # Définition du chemin de sortie
    output_base_path = os.path.join(GOLD_PATH, "securite", "bi")
    logger.info(f"Sauvegarde des tables Gold Securite BI dans {output_base_path}...")

    # Sauvegarde au format Parquet
    dim_indicateurs.write.mode("overwrite").parquet(
        os.path.join(output_base_path, "dim_indicateurs_securite")
    )
    dim_geographie.write.mode("overwrite").parquet(
        os.path.join(output_base_path, "dim_geographie_lyon")
    )
    fact_securite.write.mode("overwrite").parquet(os.path.join(output_base_path, "fact_securite"))
    fact_demographie.write.mode("overwrite").parquet(
        os.path.join(output_base_path, "fact_demographie_annuelle")
    )

    # Audit final pour vérification
    nb_indicateurs = dim_indicateurs.count()
    nb_arrondissements = dim_geographie.count()
    nb_lignes_faits = fact_securite.count()

    logger.info(
        f"Audit Gold : {nb_indicateurs} indicateurs sur {nb_arrondissements} arrondissements."
    )
    logger.info(f"Total de {nb_lignes_faits} mesures de sécurité générées.")

    logger.info("Pipeline Gold Securite BI terminée avec succès !")


if __name__ == "__main__":
    run_gold_securite_bi_pipeline()
