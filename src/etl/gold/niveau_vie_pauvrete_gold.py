import logging
import os

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH, SILVER_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_gold_niveau_vie_pauvrete_bi_pipeline():
    spark = get_spark_session("Gold_Niveau_Vie_Pauvrete_BI")
    logger.info("Demarrage de la pipeline Gold Niveau Vie Pauvrete BI...")

    base_path = os.path.join(SILVER_PATH, "niveau_vie_pauvrete")
    paths = {
        2017: os.path.join(base_path, "niveau_vie_pauvrete_2017"),
        2019: os.path.join(base_path, "niveau_vie_pauvrete_2019"),
        2021: os.path.join(base_path, "niveau_vie_pauvrete_2021"),
    }

    # Verifie que chaque dossier existe
    missing = [p for p in paths.values() if not os.path.exists(p)]
    if missing:
        logger.error(f"Dossiers Silver introuvables: {missing}")
        return

    # Lit chaque dataset + ajoute annee
    dfs = []
    for year, p in paths.items():
        df_year = spark.read.parquet(p).withColumn("year", F.lit(year).cast("int"))

        # Aligner les colonnes d'identifiants géographiques entre 2017 et 2019/2021
        if year in [2019, 2021]:
            mappings = {
                "idcar_200m": "identifiant_carreaux_au_200m",
                "idcar_1km": "id_carreaux_au_1km",
                "idcar_nat": "id_inspire_carreau_nature_dedie_au_carreau_200_m",
                "i_est_200": "identifiant_est_200m",
                "i_est_1km": "id_est_au_1km",
            }
            for old_col, new_col in mappings.items():
                if old_col in df_year.columns:
                    df_year = df_year.withColumnRenamed(old_col, new_col)

        # Cas particulier pour la casse sur une colonne 2017
        if "id_Inspire_carreau_nature_dedie_au_carreau_200_m" in df_year.columns:
            df_year = df_year.withColumnRenamed(
                "id_Inspire_carreau_nature_dedie_au_carreau_200_m",
                "id_inspire_carreau_nature_dedie_au_carreau_200_m",
            )

        # place year en premiere colonne
        df_year = df_year.select("year", *[c for c in df_year.columns if c != "year"])
        dfs.append(df_year)
        logger.info(f"{year}: {len(df_year.columns)} colonnes")

    # Merge des 3 datasets
    df_merged = dfs[0]
    for d in dfs[1:]:
        df_merged = df_merged.unionByName(d, allowMissingColumns=True)
    output_base_path = os.path.join(GOLD_PATH, "niveau_vie_pauvrete", "bi")

    df_gold = df_merged

    # Sécuriser le type de year
    if "year" in df_gold.columns:
        df_gold = df_gold.withColumn("year", F.col("year").cast("int"))
    else:
        raise ValueError("La colonne year est absente du dataset source")

    # -----------------------------
    # 1) Dimension géographie
    # -----------------------------
    geo_candidates = [
        "identifiant_carreaux_au_200m",
        "id_carreaux_au_1km",
        "id_inspire_carreau_nature_dedie_au_carreau_200_m",
        "identifiant_est_200m",
        "id_est_au_1km",
        "arrondissement",
        "commune",
        "code_commune",
        "lcog_geo_2",
        "lcog_geo_3",
        "lcog_geo_4",
        "lcog_geo_5",
    ]
    geo_cols = [c for c in geo_candidates if c in df_gold.columns]
    if not geo_cols:
        raise ValueError("Aucune colonne géographique trouvée dans le dataset source")

    dim_geographie = (
        df_gold.select(*geo_cols)
        .dropDuplicates()
        .withColumn(
            "sk_geographie",
            F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in geo_cols]),
        )
        .select("sk_geographie", *geo_cols)
    )

    # -----------------------------
    # 2) Dimension temps
    # -----------------------------
    dim_temps = (
        df_gold.select(F.col("year").alias("annee"))
        .dropDuplicates()
        .withColumn("sk_temps", F.col("annee"))
        .withColumn(
            "date_reference_annee",
            F.to_date(F.concat_ws("-", F.col("annee"), F.lit("01"), F.lit("01"))),
        )
        .withColumn("decennie", (F.floor(F.col("annee") / 10) * 10).cast("int"))
        .select("sk_temps", "annee", "date_reference_annee", "decennie")
    )

    # -----------------------------
    # 3) Table de fait
    # -----------------------------
    # Mesures = colonnes numériques hors clés/techniques
    technical_or_key_cols = set(
        geo_cols + ["year", "silver_processing_timestamp", "gold_processing_timestamp"]
    )
    measure_cols = [
        field.name
        for field in df_gold.schema.fields
        if isinstance(field.dataType, NumericType) and field.name not in technical_or_key_cols
    ]
    # Jointure avec dimensions pour récupérer les clés substituts
    fact = (
        df_gold.join(dim_geographie, on=geo_cols, how="left")
        .join(dim_temps, df_gold["year"] == dim_temps["annee"], how="left")
        .select("sk_geographie", "sk_temps", *measure_cols)
        .withColumn("gold_processing_timestamp", F.current_timestamp())
    )

    # Sorties Gold
    output_base_path = os.path.join(GOLD_PATH, "niveau_vie_pauvrete", "bi")
    gold_star_base = os.path.join(GOLD_PATH, "niveau_vie_pauvrete", "bi_star")
    os.makedirs(output_base_path, exist_ok=True)
    os.makedirs(gold_star_base, exist_ok=True)

    # -----------------------------
    # 4) Écriture des tables Gold
    # -----------------------------
    dim_geographie.write.mode("overwrite").parquet(
        os.path.join(gold_star_base, "dim_geographie_200m")
    )
    dim_temps.write.mode("overwrite").parquet(os.path.join(gold_star_base, "dim_temps"))
    fact.write.mode("overwrite").parquet(
        os.path.join(gold_star_base, "fact_niveau_vie_pauvrete_200m")
    )

    print("Schema en etoile genere avec succes")
    print(f"dim_geographie_200m: {dim_geographie.count()} lignes")
    print(f"dim_temps: {dim_temps.count()} lignes")
    print(f"fact_niveau_vie_pauvrete_200m: {fact.count()} lignes")
    print(f"Mesures retenues dans la fact: {len(measure_cols)}")

    logger.info(f"Sauvegarde de la table mergee dans {output_base_path}...")
    df_gold.write.mode("overwrite").parquet(output_base_path)

    logger.info(
        f"Pipeline terminee avec succes. Rows={df_gold.count()}, Cols={len(df_gold.columns)}"
    )


if __name__ == "__main__":
    run_gold_niveau_vie_pauvrete_bi_pipeline()
