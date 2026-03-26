import pytest
from pyspark.sql import functions as F
from src.config import BRONZE_PATH, SILVER_PATH

def get_silver_df(spark, folder_name):
    """Charge le df silver pour les tests"""
    path = f"{SILVER_PATH}/niveau_vie_pauvrete/{folder_name}"
    return spark.read.parquet(path)

@pytest.mark.parametrize("folder_name", [
    "niveau_vie_pauvrete_2017",
])
def test_silver_check_city_areas(spark, folder_name):
    """
    Vérifie que la colonne Arrondissement contient uniquement les 9 arrondissements de Lyon :
    pattern 6938X avec X de 1 à 9.
    """
    df = get_silver_df(spark, folder_name)

    # Valeurs attendues : 69381 ... 69389
    expected_values = {f"6938{i}" for i in range(1, 10)}

    # Distinct des valeurs observées en string (pour gérer int/string)
    observed_values = {
        row["Arrondissement_str"]
        for row in df.select(F.col("Arrondissement").cast("string").alias("Arrondissement_str"))
                     .distinct()
                     .collect()
    }

    # 1) Aucune valeur NULL
    null_count = df.filter(F.col("Arrondissement").isNull()).count()
    assert null_count == 0, f"{null_count} lignes ont Arrondissement=NULL"

    # 2) Toutes les valeurs respectent le pattern 6938[1-9]
    invalid_pattern_count = df.filter(
        ~F.col("Arrondissement").cast("string").rlike(r"^6938[1-9]$")
    ).count()
    assert invalid_pattern_count == 0, (
        f"{invalid_pattern_count} lignes ne respectent pas le pattern ^6938[1-9]$"
    )

    # 3) Les 9 arrondissements sont présents, et uniquement eux
    assert observed_values == expected_values, (
        f"Valeurs observées incorrectes. "
        f"observed={sorted(observed_values)} expected={sorted(expected_values)}"
    )
