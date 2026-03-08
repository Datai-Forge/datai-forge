import os
import logging
from pyspark.sql.functions import current_timestamp, expr, upper, initcap

from src.config import SILVER_PATH, PRESIDENTIELLE_BRONZE_PATH
from src.common.spark_session_manager import get_spark_session
from src.common.utils import clean_column_name

# Config du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PRESIDENTIELLE_SILVER_PATH = os.path.join(SILVER_PATH, "presidentielle")

def unpivot_candidates(df, num_candidates):
    """
    Transforme le format large (ex: 12 candidats) en format long (1 candidat par ligne).
    Utilise des noms de colonnes explicites pour le pivot.
    """
    # les 21 premières colonnes sont les infos de base
    common_cols = df.columns[:21]

    # liste des colonnes de candidats à pivoter
    # On ignore les colonnes de pourcentages pré-calculées
    candidate_fields = ["n_panneau", "sexe", "nom", "prenom", "voix"]

    stack_cols = []
    for i in range(1, num_candidates + 1):
        for field in candidate_fields:
            stack_cols.append(f"candidat_{i}_{field}")

    # Construction de l'expression SQL stack
    # stack(12, candidat_1_n_panneau, ..., candidat_12_voix) as (n_panneau, sexe, nom, prenom, voix)
    stack_expr = f"stack({num_candidates}, {', '.join(stack_cols)}) as ({', '.join(candidate_fields)})"

    # On pivote pour aller sur du format long
    unpivoted_df = df.select(*common_cols, expr(stack_expr))

    return unpivoted_df

def transform_bronze_to_silver(spark, folder_name):
    bronze_full_path = os.path.join(PRESIDENTIELLE_BRONZE_PATH, folder_name)
    silver_full_path = os.path.join(PRESIDENTIELLE_SILVER_PATH, folder_name)

    if not os.path.exists(bronze_full_path):
        logger.error(f"Dossier Bronze introuvable : {bronze_full_path}")
        return

    logger.info(f"Début de la transformation Silver pour : {folder_name}")

    df = spark.read.parquet(bronze_full_path)

    # renommage explicite des colonnes candidats des candidats
    # Liste des attributs dans l'ordre du fichier source
    attr_names = ["n_panneau", "sexe", "nom", "prenom", "voix", "pct_ins", "pct_exp"]

    # Nettoyage des noms des 21 premières colonnes
    new_names = [clean_column_name(c) for c in df.columns[:21]]

    # Calcul du nombre de candidats (colonnes restantes / 7)
    # On ignore les 2 colonnes techniques de la couche bronze à la fin
    candidate_cols_raw = [c for c in df.columns[21:] if c not in ['source_file', 'bronze_processing_timestamp']]
    num_candidates = len(candidate_cols_raw) // 7

    logger.info(f"Nombre de candidats détectés : {num_candidates}")

    for i in range(1, num_candidates + 1):
        for attr in attr_names:
            new_names.append(f"candidat_{i}_{attr}")

    # On rajoute les noms des colonnes techniques à la fin s'ils étaient là
    if 'source_file' in df.columns: new_names.append('source_file')
    if 'bronze_processing_timestamp' in df.columns: new_names.append('bronze_processing_timestamp')

    # application des nouveaux noms de colonnes
    df = df.toDF(*new_names)
    logger.info(f"Renommage explicite effectué : candidat_1_{attr_names[0]}...candidat_{num_candidates}_{attr_names[-1]}")

    # Dépivotage
    df = unpivot_candidates(df, num_candidates)

    # Casting et nettoyage
    # On ne caste que les colonnes brutes
    # Les colonnes calculées ) sont ignorées
    # car supression
    df = df.withColumn("voix", df["voix"].cast("int")) \
           .withColumn("inscrits", df["inscrits"].cast("int")) \
           .withColumn("abstentions", df["abstentions"].cast("int")) \
           .withColumn("votants", df["votants"].cast("int")) \
           .withColumn("exprimes", df["exprimes"].cast("int")) \
           .withColumn("nom", upper(df["nom"])) \
           .withColumn("prenom", initcap(df["prenom"])) \
           .withColumn("silver_processing_timestamp", current_timestamp())

    # On ne garde que les colonnes propres et on ignore les colonnes calculées
    final_cols = [
        "code_du_departement", "libelle_du_departement", "code_de_la_commune", "libelle_de_la_commune",
        "code_du_b_vote", "inscrits", "abstentions", "votants", "exprimes",
        "n_panneau", "sexe", "nom", "prenom", "voix", "silver_processing_timestamp"
    ]

    # On filtre les colonnes finales pour s'assurer qu'elles existent
    df = df.select([c for c in final_cols if c in df.columns])

    # Checksum
    # Somme des exprimés
    # Comme la colonne 'exprimes' est répétée num_candidates fois après l'unpivot,
    # on prend le distinct par bureau pour avoir la vraie somme source.
    total_expected = df.select("code_du_b_vote", "exprimes").distinct().agg({"exprimes": "sum"}).collect()[0][0]

    # Somme des voix (total réel obtenu après transformation)
    total_actual = df.agg({"voix": "sum"}).collect()[0][0]

    if total_expected == total_actual:
        logger.info(f"✅ Checksum Validé : {total_actual} voix au total.")
    else:
        logger.error(f"❌ ERREUR DE CHECKSUM : Attendu {total_expected}, Obtenu {total_actual}")

    # Écriture
    if not os.path.exists(PRESIDENTIELLE_SILVER_PATH):
        os.makedirs(PRESIDENTIELLE_SILVER_PATH, exist_ok=True)

    df.write.mode("overwrite").parquet(silver_full_path)

    logger.info(f"Transformation terminée avec succès : {silver_full_path}")
    logger.info(f"Nombre de lignes finales : {df.count()}")
    df.printSchema()

if __name__ == "__main__":
    spark = get_spark_session(app_name="Silver_Presidentielle_Explicit_Unpivot")

    try:
        transform_bronze_to_silver(spark, "lyon_T1_presidentiel_2022")
        transform_bronze_to_silver(spark, "lyon_T2_presidentiel_2022")
    finally:
        spark.stop()
        logger.info("Pipeline Silver terminée proprement.")
