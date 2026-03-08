import pytest
from pyspark.sql import functions as F
from src.config import BRONZE_PATH, SILVER_PATH

def get_silver_df(spark, folder_name):
    """Charge le df silver pour les tests"""
    path = f"{SILVER_PATH}/presidentielle/{folder_name}"
    return spark.read.parquet(path)

@pytest.mark.parametrize("folder_name", [
    "lyon_T1_presidentiel_2022",
    "lyon_T2_presidentiel_2022"
])
def test_silver_votes_checksum(spark, folder_name):
    """
    Vérification de l'intégrité des voix :
    La somme des voix individuelles doit être égale au total des exprimés.
    """
    df = get_silver_df(spark, folder_name)

    # Total des exprimés
    # Comme la donnée est répétée n fois après le dépivotage, on prend le distinct par bureau
    total_expected = df.select("code_du_b_vote", "exprimes").distinct().agg(F.sum("exprimes")).collect()[0][0]

    # Somme des voix réelles (total après la transformation)
    total_actual = df.agg(F.sum("voix")).collect()[0][0]

    assert total_expected == total_actual, \
        f"Erreur de Checksum pour {folder_name} : Attendu {total_expected}, Obtenu {total_actual}"

@pytest.mark.parametrize("folder_name", [
    "lyon_T1_presidentiel_2022",
    "lyon_T2_presidentiel_2022"
])
def test_silver_row_count_integrity(spark, folder_name):
    """
    Vérification dynamique de la multiplication des lignes :
    Le nombre de lignes en Silver doit être égal au (Nombre de Bureaux en Bronze) * (Nombre de Candidats).
    """
    # Chargement bronze et Silver
    bronze_df = spark.read.parquet(f"{BRONZE_PATH}/presidentielle/{folder_name}")
    silver_df = spark.read.parquet(f"{SILVER_PATH}/presidentielle/{folder_name}")

    # On calcule dynamiquement le nombre de candidats dans bronze
    # Dans les fichiers officiels élection, on a :
    # - 21 colonnes de base
    # - n candidats avec 7 attributs chacun (N°Panneau, Sexe, Nom, Prénom, Voix, % Voix/Ins, % Voix/Exp)
    # - 2 colonnes de métadonnées (source_file, bronze_processing_timestamp)

    total_cols = len(bronze_df.columns)
    #  La formule c'est (Total - Fixes début - Métadonnées fin) / 7
    num_candidates = (total_cols - 21 - 2) // 7

    num_bronze_rows = bronze_df.count()

    # Calcule du total attendu (Bureaux * Candidats)
    expected_rows = num_bronze_rows * num_candidates

    actual_rows = silver_df.count()

    assert actual_rows == expected_rows, \
        f"Erreur d'intégrité pour {folder_name} : Attendu {expected_rows} lignes ({num_bronze_rows} bureaux x {num_candidates} candidats), Obtenu {actual_rows}"
