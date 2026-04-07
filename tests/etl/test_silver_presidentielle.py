import pytest
from pyspark.sql import functions as F

from src.config import BRONZE_PATH, SILVER_PATH


def get_silver_df(spark, folder_name):
    """Charge le df silver pour les tests"""
    path = f"{SILVER_PATH}/presidentielle/{folder_name}"
    return spark.read.parquet(path)


@pytest.mark.parametrize("folder_name", ["lyon_T1_presidentiel_2022", "lyon_T2_presidentiel_2022"])
def test_silver_votes_checksum(spark, folder_name):
    """
    Vérification de l'intégrité des voix :
    La somme des voix individuelles doit être égale au total des exprimés.
    """
    df = get_silver_df(spark, folder_name)

    # Total des exprimés
    # Comme la donnée est répétée n fois après le dépivotage, on prend le distinct par bureau
    total_expected = (
        df.select("code_du_b_vote", "exprimes").distinct().agg(F.sum("exprimes")).collect()[0][0]
    )

    # Somme des voix réelles (total après la transformation)
    total_actual = df.agg(F.sum("voix")).collect()[0][0]

    assert (
        total_expected == total_actual
    ), f"Erreur de Checksum pour {folder_name} : Attendu {total_expected}, Obtenu {total_actual}"


@pytest.mark.parametrize("folder_name", ["lyon_T1_presidentiel_2022", "lyon_T2_presidentiel_2022"])
def test_silver_row_count_integrity(spark, folder_name):
    """
    Vérification dynamique de la multiplication des lignes :
    Silver count == (Nombre de Bureaux en Bronze) * (Nombre de Candidats).
    """
    # Chargement bronze et Silver
    bronze_path = f"{BRONZE_PATH}/presidentielle/{folder_name}"
    silver_path = f"{SILVER_PATH}/presidentielle/{folder_name}"
    bronze_df = spark.read.parquet(bronze_path)
    silver_df = spark.read.parquet(silver_path)

    # On calcule dynamiquement le nombre de candidats dans bronze
    # Dans les fichiers officiels élection, on a :
    # - 21 colonnes de base
    # - n candidats avec 7 attributs chacun (Voix, % Voix/Exp, etc.)
    # - 2 colonnes de métadonnées (source_file, bronze_processing_timestamp)

    total_cols = len(bronze_df.columns)
    #  La formule c'est (Total - Fixes début - Métadonnées fin) / 7
    num_candidates = (total_cols - 21 - 2) // 7

    num_bronze_rows = bronze_df.count()

    # Calcule du total attendu (Bureaux * Candidats)
    expected_rows = num_bronze_rows * num_candidates

    actual_rows = silver_df.count()

    assert actual_rows == expected_rows, (
        f"Erreur d'intégrité pour {folder_name} : "
        f"Attendu {expected_rows} ({num_bronze_rows} x {num_candidates}), "
        f"Obtenu {actual_rows}"
    )


@pytest.mark.parametrize("folder_name", ["lyon_T1_presidentiel_2022", "lyon_T2_presidentiel_2022"])
def test_silver_political_enrichment_integrity(spark, folder_name):
    """
    Vérifie que les données d'enrichissement politique sont bien présentes et complètes.
    """
    df = get_silver_df(spark, folder_name)

    new_cols = ["parti_code", "parti_nom", "nuance_officielle", "bloc_analytique"]

    # Pour vérifier la présence des colonnes
    for col in new_cols:
        assert (
            col in df.columns
        ), f"La colonne {col} est absente du schéma Silver pour {folder_name}."

    # On vérifie qu'il n'y a pas de null
    null_count = df.filter(
        F.col("parti_code").isNull()
        | F.col("parti_nom").isNull()
        | F.col("nuance_officielle").isNull()
        | F.col("bloc_analytique").isNull()
    ).count()

    assert null_count == 0, (
        f"Erreur d'audit : {null_count} lignes avec données politiques manquantes "
        f"dans {folder_name}. Vérifiez le mapping CSV."
    )


@pytest.mark.parametrize("folder_name", ["lyon_T1_presidentiel_2022", "lyon_T2_presidentiel_2022"])
def test_silver_insee_integrity(spark, folder_name):
    """
    Vérifie la validité du code INSEE de l'arrondissement.
    - La colonne doit être présente.
    - Les codes doivent être compris entre 69381 et 69389.
    - Le bureau 0001 doit être rattaché au 69381.
    """
    df = get_silver_df(spark, folder_name)

    # check de la colonne
    assert "code_insee_arrondissement" in df.columns

    # Format des codes (69381 à 69389)
    invalid_codes = df.filter(~F.col("code_insee_arrondissement").rlike("^6938[1-9]$")).count()
    assert invalid_codes == 0, f"Codes INSEE invalides détectés dans {folder_name}."

    # Règle métier spécifique au bureau 0001 (Hôtel de Ville -> 1er arr + Type Administratif)
    bureau_0001 = df.filter(F.col("code_du_b_vote") == "0001").collect()
    if bureau_0001:
        assert bureau_0001[0]["code_insee_arrondissement"] == "69381"
        assert bureau_0001[0]["type_bureau"] == "ADMINISTRATIF"

    # Vérifier qu'un bureau classique est bien ordinaire
    bureau_ordinaire = df.filter(F.col("code_du_b_vote") != "0001").limit(1).collect()
    if bureau_ordinaire:
        assert bureau_ordinaire[0]["type_bureau"] == "ORDINAIRE"
