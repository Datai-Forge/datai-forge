import os
import logging
from pyspark.sql.functions import current_timestamp, expr, upper, initcap
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col

from src.config import SILVER_PATH
from src.common.spark_session_manager import get_spark_session
from src.common.utils import clean_column_name
from src.config import BRONZE_PATH

# Config du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

NIVEAU_VIE_PAUVRETE_SILVER_PATH = os.path.join(SILVER_PATH, "niveau_vie_pauvrete")

def transform_bronze_to_silver(spark, folder_name):
    # bronze_full_path = os.path.join(NIVEAU_VIE_PAUVRETE_BRONZE_PATH, folder_name)
    silver_full_path = os.path.join(NIVEAU_VIE_PAUVRETE_SILVER_PATH, folder_name)

    # if not os.path.exists(bronze_full_path):
    #     logger.error(f"Dossier Bronze introuvable : {bronze_full_path}")
    #     return

    logger.info(f"Début de la transformation Silver pour : {folder_name}")

    nvp_2017_bronze_path = os.path.join(BRONZE_PATH, "niveau_vie_pauvrete_200m", "2017_carreaux_200m_met")
    df_silver_2017 = spark.read.parquet(nvp_2017_bronze_path)

    # # Convert columns from 'Ind' to 'Men_pauv' to integer type.
    # # This assumes 'Ind' and 'Men_pauv' exist and define a contiguous range of columns
    # # in the DataFrame's schema.
    df_silver_2017 = df_silver_2017.withColumn("Ind", F.col("Ind").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men_1ind", F.col("Men_1ind").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men_5ind", F.col("Men_5ind").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men_prop", F.col("Men_prop").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men_fmp", F.col("Men_fmp").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_snv", F.col("Ind_snv").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men_surf", F.col("Men_surf").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men_coll", F.col("Men_coll").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men_mais", F.col("Men_mais").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Log_av45", F.col("Log_av45").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Log_45_70", F.col("Log_45_70").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Log_70_90", F.col("Log_70_90").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Log_ap90", F.col("Log_ap90").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Log_inc", F.col("Log_inc").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Log_soc", F.col("Log_soc").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_0_3", F.col("Ind_0_3").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_4_5", F.col("Ind_4_5").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_6_10", F.col("Ind_6_10").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_11_17", F.col("Ind_11_17").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_18_24", F.col("Ind_18_24").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_25_39", F.col("Ind_25_39").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_40_54", F.col("Ind_40_54").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_55_64", F.col("Ind_55_64").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_65_79", F.col("Ind_65_79").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_80p", F.col("Ind_80p").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Ind_inc", F.col("Ind_inc").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men_pauv", F.col("Men_pauv").cast("float"))
    df_silver_2017 = df_silver_2017.withColumn("Men", F.col("Men").cast("float"))

    # split column lcog_geo into 2 columns, the second column will be named "arrondissement limitrophe" string tyed
    max_lcog_geo_chars = (
    df_silver_2017
        .select(F.max(F.length(F.col("lcog_geo").cast("string"))).alias("max_char_count"))
        .collect()[0]["max_char_count"]
    )

    # Split lcog_geo into 5 zipcode columns (5 chars each) from df_bronze_2017
    chunk_size = 5
    num_chunks = max_lcog_geo_chars // chunk_size  # 25 // 5 = 5

    df_bronze_2017_split = df_silver_2017.withColumn(
        "lcog_geo_clean",
        F.regexp_replace(F.col("lcog_geo").cast("string"), r"\D", "")  # keep digits only
    )

    for i in range(num_chunks):
        start_pos = i * chunk_size + 1
        end_needed = start_pos + chunk_size - 1
        df_bronze_2017_split = df_bronze_2017_split.withColumn(
            f"lcog_geo_{i+1}",
            F.when(
                F.length(F.col("lcog_geo_clean")) >= end_needed,
                F.substring(F.col("lcog_geo_clean"), start_pos, chunk_size)
            )
        )
    df_bronze_2017_split = df_bronze_2017_split.drop("lcog_geo_clean")
    df_silver_2017 = df_bronze_2017_split
    # quick check
    df_silver_2017.select("lcog_geo", "lcog_geo_1", "lcog_geo_2", "lcog_geo_3", "lcog_geo_4", "lcog_geo_5").show(20, truncate=False)

    # rename columns header
    new_names = {
        "Idcar_200m":"Identifiant_carreaux_au_200m",
        "I_est_200":"Identifiant_est_200m",
        "Idcar_1km":"Id_carreaux_au_1km",
        "I_est_1km":"Id_est_au_1km",
        "Idcar_nat":"Id_Inspire_carreau_nature_dedie_au_carreau_200_m",
        "Groupe":"Numéro_groupe_dedie_au_carreau",
        "Ind":"Nb_individus",
        "Men_1ind":"Nb_menages_a_un_seul_individu",
        "Men_5ind":"Nb_menages_a_5_individus_ou_plus",
        "Men_prop":"Nb_menages_propriétaires",
        "Men_fmp":"Nb_menages_monoparentaux",
        "Ind_snv":"Somme_niveaux_de_vie_winsorises_des_individus",
        "Men_surf":"Somme_surface_logements_du_carreau",
        "Men_coll":"Nb_menages_en_logements_collectifs",
        "Men_mais":"Nb_menages_en_maison",
        "Log_av45":"Nb_logements_construits_avant_1945",
        "Log_45_70":"Nb_logements_construits_entre_de_1945-1970",
        "Log_70_90":"Nb_logements_construits_de_1970-1990",
        "Log_ap90":"Nb_logements_construits_apres_1990",
        "Log_inc":"Nb_logements_date_construction_inconnue",
        "Log_soc":"Nb_logements_sociaux",
        "Ind_0_3":"Nb_individus_0-3_ans",
        "Ind_4_5":"Nb_individus_4-5_ans",
        "Ind_6_10":"Nb_individus_6-10_ans",
        "Ind_11_17":"Nb_individus_11-17_ans",
        "Ind_18_24":"Nb_individus_18-24_ans",
        "Ind_25_39":"Nb_individus_de_25-39_ans",
        "Ind_40_54":"Nb_individus_40-54_ans",
        "Ind_55_64":"Nb_individus_55-64_ans",
        "Ind_65_79":"Nb_individus_65-79_ans",
        "Ind_80p":"Nb_individus_+80_ans",
        "Ind_inc":"Nb_individus_age_inconnu",
        "Men_pauv":"Nb_menages_pauvres",
        "Men":"Nb_menages",
        "lcog_geo_1":"Arrondissement"
    }
    # Use select with an alias for each column
    df_silver_2017 = df_silver_2017.select([df_silver_2017[c].alias(new_names.get(c, c)) for c in df_silver_2017.columns])

    # add boolean column, named "est_limitrophe", with values 0 when the condition it's false and 1 when condition it's true
    df_silver_2017 = df_silver_2017.withColumn(
    "est_limitrophe",
        when(
            (col("Arrondissement").isNotNull()) &
            (
                col("lcog_geo_2").isNotNull() |
                col("lcog_geo_3").isNotNull() |
                col("lcog_geo_4").isNotNull() |
                col("lcog_geo_5").isNotNull()
            ),
            1
        ).otherwise(0)
    )
    df_silver_2017.select(
        "Arrondissement", "lcog_geo_2", "lcog_geo_3", "lcog_geo_4", "lcog_geo_5", "est_limitrophe"
    ).show(20, truncate=False)

    # add code insee commune column
    df_silver_2017 = df_silver_2017

    # add commune column
    df_silver_2017 = df_silver_2017.withColumn("commune", F.lit("Lyon"))

    # add code insee departement column
    df_silver_2017 = df_silver_2017.withColumn("code_commune", F.lit("69123"))

    # add tomestamp and save in silver
    df_silver_2017 = df_silver_2017.withColumn("silver_processing_timestamp", current_timestamp())
    df_silver_2017.write.mode("overwrite").parquet(silver_full_path)
    logger.info(f"Transformation terminée avec succès : {silver_full_path}")

    logger.info(f"Nb de lignes finales : {df_silver_2017.count()}")
    df_silver_2017.printSchema()

if __name__ == "__main__":
    spark = get_spark_session(app_name="Silver_Presidentielle_Explicit_Unpivot")

    try:
        transform_bronze_to_silver(spark, "niveau_vie_pauvrete_2017")
    finally:
        spark.stop()
        logger.info("Pipeline Silver terminée proprement.")
