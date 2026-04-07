import os

import pytest
from pyspark.sql import functions as F

from src.config import GOLD_PATH


def get_gold_ml_df(spark, dataset_name):
    """Charge un dataset Gold ML pour les tests"""
    path = os.path.join(GOLD_PATH, "ml", dataset_name)
    if not os.path.exists(path):
        pytest.skip(f"Dataset {dataset_name} non généré, skipping test.")
    return spark.read.parquet(path)


def test_obt_base_elections_integrity(spark):
    """Validation du socle électoral (Étape 1)"""
    df = get_gold_ml_df(spark, "base_elections_analytical")

    # Nombre de lignes : (305 bureaux total - 1 exclu) * 2 tours = 608
    assert df.count() == 608
    # Exclusion du bureau administratif 0001
    assert df.filter(F.col("id_bureau") == "0001").count() == 0
    # Présence des targets majeurs
    for target in [
        "target_score_gauche_pct",
        "target_score_centre_pct",
        "target_score_extreme_droite_pct",
    ]:
        assert target in df.columns


def test_obt_security_enrichment_integrity(spark):
    """Validation de l'enrichissement Sécurité (Étape 2)"""
    df = get_gold_ml_df(spark, "obt_step2_security")

    # Grain conservé
    assert df.count() == 608
    # Présence des 3 piliers
    piliers = ["violence", "propriete", "rue_stup"]
    for p in piliers:
        assert f"feat_secu_{p}_2021" in df.columns
        assert f"feat_secu_{p}_delta_5ans" in df.columns
    # Absence de NULLs sur la sécurité
    assert df.filter(F.col("feat_secu_violence_2021").isNull()).count() == 0


def test_obt_ml_complete_integrity(spark):
    """
    Validation finale de l'OBT finalisé :
    - Intégrité du grain final (608 lignes)
    - Complétude des données sociales (Zéro NULL)
    - Cohérence métier des revenus et deltas
    """
    df = get_gold_ml_df(spark, "obt_ml_complete")

    # Validation du grain final
    n_rows = df.count()
    assert n_rows == 608, f"Le dataset final a perdu des lignes : {n_rows} au lieu de 608."

    # Validation de l'enrichissement social (0 null attendu)
    social_features = [
        "feat_social_revenu_moyen",
        "feat_social_taux_pauvrete",
        "feat_social_pct_proprietaires",
        "feat_social_delta_revenu_5ans",
    ]
    for feat in social_features:
        assert feat in df.columns, f"La feature sociale {feat} est manquante."
        null_count = df.filter(F.col(feat).isNull()).count()
        assert (
            null_count == 0
        ), f"Présence de {null_count} NULL dans {feat}. Jointure arrondissement échouée ?"

    # Sanity Check : Cohérence métier
    # Le revenu moyen à Lyon par arrondissement ne peut pas être < 10000 ou > 100000
    stats = df.select(
        F.min("feat_social_revenu_moyen").alias("min_rev"),
        F.max("feat_social_taux_pauvrete").alias("max_pauv"),
    ).collect()[0]

    assert stats["min_rev"] > 10000, f"Revenu trop bas détecté : {stats['min_rev']}"
    assert stats["max_pauv"] < 50, f"Pauvreté aberrante : {stats['max_pauv']}%"

    # Vérification de la somme des scores (Cohérence mathématique)
    # On vérifie que la somme des blocs est proche de 100% au T1
    df_sums = df.filter(F.col("tour") == 1).withColumn(
        "total_pct",
        F.col("target_score_gauche_pct")
        + F.col("target_score_centre_pct")
        + F.col("target_score_droite_pct")
        + F.col("target_score_extreme_gauche_pct")
        + F.col("target_score_extreme_droite_pct"),
    )

    # On autorise une marge pour les petits candidats non classés
    invalid_sums = df_sums.filter((F.col("total_pct") < 85) | (F.col("total_pct") > 101)).count()
    assert (
        invalid_sums == 0
    ), f"Cohérence des scores électoraux en échec pour {invalid_sums} bureaux."
