import logging
import os

from pyspark.sql import functions as F

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH, SILVER_PATH

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_gold_ml_step2_enrichment_security():
    """
    Étape 2 : Enrichissement de la base électorale avec les données de Sécurité.
    - Création des 3 Piliers (Violence, Propriété, Rue/Stup).
    - Calcul des Deltas temporels (3 ans et 5 ans).
    - Jointure avec le socle électoral (OBT).
    """
    spark = get_spark_session("Gold_ML_Step2_Security")
    logger.info("Démarrage de l'étape 2 : Enrichissement Sécurité...")

    # Chargement des données
    base_election_path = os.path.join(GOLD_PATH, "ml", "base_elections_analytical")
    security_silver_path = os.path.join(SILVER_PATH, "securite", "lyon_securite")

    df_election = spark.read.parquet(base_election_path)
    df_secu = spark.read.parquet(security_silver_path)

    # Définition des Piliers (Mapping SSMSI -> CEVIPOF)
    mapping_piliers = {
        "violence": ["Violences physiques hors cadre familial", "Violences sexuelles"],
        "propriete": [
            "Cambriolages de logement",
            "Vols de véhicule",
            "Vols dans les véhicules",
            "Vols d'accessoires sur véhicules",
            "Destructions et dégradations volontaires",
        ],
        "rue_stup": ["Usage de stupéfiants", "Trafic de stupéfiants"],
    }

    # Préparation des données de sécurité par Pilier et par Année
    df_secu = df_secu.withColumn("annee", F.year("date_reference"))

    # On crée une colonne pilier à partir de l'indicateur
    df_secu_piliers = df_secu.withColumn(
        "pilier",
        F.when(F.col("indicateur").isin(mapping_piliers["violence"]), "violence")
        .when(F.col("indicateur").isin(mapping_piliers["propriete"]), "propriete")
        .when(F.col("indicateur").isin(mapping_piliers["rue_stup"]), "rue_stup")
        .otherwise(None),
    ).filter(F.col("pilier").isNotNull())

    # Aggrégation par Arrondissement / Année / Pilier
    df_agg = df_secu_piliers.groupBy("code_geo", "annee", "pilier").agg(
        F.sum("taux_pour_1000").alias("taux_pilier")
    )

    # Pivotement pour avoir les piliers en colonnes par année
    # On veut : code_geo | annee | taux_violence | taux_propriete | taux_rue_stup
    df_pivot = df_agg.groupBy("code_geo", "annee").pivot("pilier").agg(F.first("taux_pilier"))

    # Calcul des Deltas et Snapshot 2021
    # On sépare les années pour calculer les évolutions
    df_2017 = df_pivot.filter(F.col("annee") == 2017).select(
        F.col("code_geo"),
        F.col("violence").alias("v17"),
        F.col("propriete").alias("p17"),
        F.col("rue_stup").alias("s17"),
    )
    df_2019 = df_pivot.filter(F.col("annee") == 2019).select(
        F.col("code_geo"),
        F.col("violence").alias("v19"),
        F.col("propriete").alias("p19"),
        F.col("rue_stup").alias("s19"),
    )
    df_2021 = df_pivot.filter(F.col("annee") == 2021).select(
        F.col("code_geo"),
        F.col("violence").alias("feat_secu_violence_2021"),
        F.col("propriete").alias("feat_secu_propriete_2021"),
        F.col("rue_stup").alias("feat_secu_rue_stup_2021"),
    )

    # Fusion des années
    df_features_secu = df_2021.join(df_2017, "code_geo", "inner").join(df_2019, "code_geo", "inner")

    # Calcul des Deltas (Evolution relative en %)
    # Formule : ((V2021 - Vprev) / Vprev) * 100
    for p, name in [("v", "violence"), ("p", "propriete"), ("s", "rue_stup")]:
        col_2021 = f"feat_secu_{name}_2021"
        # Delta 5 ans (2021 vs 2017)
        df_features_secu = df_features_secu.withColumn(
            f"feat_secu_{name}_delta_5ans",
            F.round(((F.col(col_2021) - F.col(f"{p}17")) / F.col(f"{p}17")) * 100, 2),
        )
        # Delta 3 ans (2021 vs 2019)
        df_features_secu = df_features_secu.withColumn(
            f"feat_secu_{name}_delta_3ans",
            F.round(((F.col(col_2021) - F.col(f"{p}19")) / F.col(f"{p}19")) * 100, 2),
        )

    # Nettoyage : on ne garde que les colonnes finales
    cols_to_keep = ["code_geo"] + [
        c for c in df_features_secu.columns if c.startswith("feat_secu_")
    ]
    df_final_secu = df_features_secu.select(*cols_to_keep)

    # Jointure avec la base électorale
    logger.info("Fusion de la sécurité avec l'OBT électorale...")
    df_obt = df_election.join(
        df_final_secu, df_election.code_insee_arrondissement == df_final_secu.code_geo, "inner"
    ).drop("code_geo")

    # Sauvegarde
    output_path = os.path.join(GOLD_PATH, "ml", "obt_step2_security")
    logger.info(f"Sauvegarde de l'OBT enrichie (Sécurité) dans {output_path}...")
    df_obt.write.mode("overwrite").parquet(output_path)

    # Audit final
    n_rows = df_obt.count()
    logger.info(f"Étape 2 terminée : {n_rows} lignes générées.")
    df_obt.show(5)


if __name__ == "__main__":
    run_gold_ml_step2_enrichment_security()
