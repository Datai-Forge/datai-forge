import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

from src.config import SILVER_PATH


def get_silver_df(spark, folder_name):
    """Charge le df silver pour les tests"""
    path = f"{SILVER_PATH}/niveau_vie_pauvrete/{folder_name}"
    return spark.read.parquet(path)


@pytest.mark.parametrize(
    "folder_name",
    [
        "niveau_vie_pauvrete_2017",
        "niveau_vie_pauvrete_2019",
        "niveau_vie_pauvrete_2021",
    ],
)
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
    assert (
        invalid_pattern_count == 0
    ), f"{invalid_pattern_count} lignes ne respectent pas le pattern ^6938[1-9]$"

    # 3) Les 9 arrondissements sont présents, et uniquement eux
    assert observed_values == expected_values, (
        f"Valeurs observées incorrectes. "
        f"observed={sorted(observed_values)} expected={sorted(expected_values)}"
    )


def reorder_like_reference(df, ref_cols, keep_extra_cols=False):
    for c in ref_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))

    if keep_extra_cols:
        extra_cols = [c for c in df.columns if c not in ref_cols]
        ordered_cols = ref_cols + extra_cols
    else:
        ordered_cols = ref_cols

    return df.select(*ordered_cols)


def test_silver_nvp_columns_order_matches_2017_reference(spark):
    ref_path = f"{SILVER_PATH}/niveau_vie_pauvrete/niveau_vie_pauvrete_2017"
    path_2019 = f"{SILVER_PATH}/niveau_vie_pauvrete/niveau_vie_pauvrete_2019"
    path_2021 = f"{SILVER_PATH}/niveau_vie_pauvrete/niveau_vie_pauvrete_2021"

    df_ref = spark.read.parquet(ref_path)
    ref_cols = df_ref.columns

    df_2019 = spark.read.parquet(path_2019)
    df_2021 = spark.read.parquet(path_2021)

    df_2019_ordered = reorder_like_reference(df_2019, ref_cols, keep_extra_cols=False)
    df_2021_ordered = reorder_like_reference(df_2021, ref_cols, keep_extra_cols=False)

    assert (
        df_2019_ordered.columns == ref_cols
    ), "L ordre des colonnes 2019 ne correspond pas a la reference 2017"
    assert (
        df_2021_ordered.columns == ref_cols
    ), "L ordre des colonnes 2021 ne correspond pas a la reference 2017"
