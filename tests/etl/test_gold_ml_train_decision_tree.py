import pandas as pd
from pyspark.sql import Row

from src.etl.gold.gold_ml_train_decision_tree import (
    TARGET_COLS,
    build_classification_frame,
    build_regression_frame,
    evaluate_classifier,
    evaluate_regressor,
    get_feature_columns,
    split_by_bureau_group,
    train_classifier,
    train_regressor,
)


def test_get_feature_columns_keeps_only_feat_prefix():
    cols = ["feat_a", "feat_b", "target_score_gauche_pct", "id_bureau", "tour"]
    assert get_feature_columns(cols) == ["feat_a", "feat_b"]


def test_build_classification_frame_creates_non_null_label(spark):
    rows = [
        Row(
            feat_a=1.0,
            feat_b=2.0,
            id_bureau="0002",
            tour=1,
            target_score_gauche_pct=40.0,
            target_score_centre_pct=20.0,
            target_score_droite_pct=15.0,
            target_score_extreme_gauche_pct=10.0,
            target_score_extreme_droite_pct=15.0,
        ),
        Row(
            feat_a=2.0,
            feat_b=3.0,
            id_bureau="0003",
            tour=2,
            target_score_gauche_pct=18.0,
            target_score_centre_pct=12.0,
            target_score_droite_pct=30.0,
            target_score_extreme_gauche_pct=7.0,
            target_score_extreme_droite_pct=33.0,
        ),
    ]
    df = spark.createDataFrame(rows)
    feature_cols = get_feature_columns(df.columns)

    out = build_classification_frame(df, feature_cols)
    assert "label" in out.columns
    assert out.filter("label IS NULL").count() == 0
    labels = {r["label"] for r in out.select("label").collect()}
    assert labels.issubset(set(TARGET_COLS))


def test_build_regression_frame_keeps_requested_target(spark):
    rows = [
        Row(
            feat_a=1.0,
            feat_b=2.0,
            id_bureau="0002",
            tour=1,
            target_score_extreme_droite_pct=15.0,
        )
    ]
    df = spark.createDataFrame(rows)
    feature_cols = get_feature_columns(df.columns)

    out = build_regression_frame(df, feature_cols, "target_score_extreme_droite_pct")
    assert out.columns == ["feat_a", "feat_b", "id_bureau", "tour", "label"]


def test_split_by_bureau_group_returns_expected_shapes(spark):
    rows = [
        Row(feat_a=float(i), feat_b=float(i + 1), id_bureau=f"{i:04d}", tour=1, label="A")
        for i in range(1, 16)
    ]
    df = spark.createDataFrame(rows)
    x_train, y_train, x_test, y_test = split_by_bureau_group(
        df,
        feature_cols=["feat_a", "feat_b"],
        task="classification",
    )

    assert len(x_train) > 0
    assert len(x_test) > 0
    assert list(x_train.columns) == ["feat_a", "feat_b"]
    assert y_train.dtype == object
    assert y_test.dtype == object


def test_classifier_training_and_metrics_run():
    x_train = pd.DataFrame(
        {
            "feat_a": [0.0, 0.2, 0.8, 1.0],
            "feat_b": [0.1, 0.3, 0.7, 0.9],
        }
    )
    y_train = pd.Series(["L", "L", "R", "R"])
    x_test = pd.DataFrame({"feat_a": [0.15, 0.85], "feat_b": [0.2, 0.8]})
    y_test = pd.Series(["L", "R"])

    model = train_classifier(
        x_train,
        y_train,
        max_depth=5,
        min_samples_leaf=1,
        ccp_alpha=0.0,
        random_state=42,
    )
    metrics = evaluate_classifier(model, x_test, y_test)

    assert "f1_macro" in metrics
    assert "balanced_accuracy" in metrics
    assert 0.0 <= metrics["f1_macro"] <= 1.0
    assert 0.0 <= metrics["balanced_accuracy"] <= 1.0


def test_regressor_training_and_metrics_run():
    x_train = pd.DataFrame(
        {
            "feat_a": [0.0, 0.2, 0.8, 1.0],
            "feat_b": [0.1, 0.3, 0.7, 0.9],
        }
    )
    y_train = pd.Series([5.0, 10.0, 25.0, 30.0])
    x_test = pd.DataFrame({"feat_a": [0.15, 0.85], "feat_b": [0.2, 0.8]})
    y_test = pd.Series([8.0, 28.0])

    model = train_regressor(
        x_train,
        y_train,
        max_depth=5,
        min_samples_leaf=1,
        ccp_alpha=0.0,
        random_state=42,
    )
    metrics = evaluate_regressor(model, x_test, y_test)

    assert "mae" in metrics
    assert "rmse" in metrics
    assert "r2" in metrics
    assert metrics["mae"] >= 0.0
    assert metrics["rmse"] >= 0.0
