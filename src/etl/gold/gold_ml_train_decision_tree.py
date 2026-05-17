import argparse
import json
import logging
import math
import os

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sklearn.metrics import (
    balanced_accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor, export_text

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TARGET_COLS = [
    "target_score_gauche_pct",
    "target_score_centre_pct",
    "target_score_droite_pct",
    "target_score_extreme_gauche_pct",
    "target_score_extreme_droite_pct",
]


def get_feature_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if c.startswith("feat_")]


def build_classification_frame(df: DataFrame, feature_cols: list[str]) -> DataFrame:
    winner_struct = F.array_max(
        F.array(
            *[
                F.struct(F.col(c).cast("double").alias("score"), F.lit(c).alias("bloc"))
                for c in TARGET_COLS
            ]
        )
    )
    return (
        df.withColumn("label", winner_struct["bloc"])
        .select(*feature_cols, "id_bureau", "tour", "label")
        .filter(F.col("label").isNotNull())
    )


def build_regression_frame(df: DataFrame, feature_cols: list[str], target_col: str) -> DataFrame:
    return df.select(*feature_cols, "id_bureau", "tour", F.col(target_col).alias("label"))


def split_by_bureau_group(
    df_model: DataFrame,
    feature_cols: list[str],
    task: str,
    test_bucket: int = 4,
    modulo: int = 5,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    df_split = df_model.withColumn(
        "split_key",
        (F.abs(F.hash(F.col("id_bureau"))) % F.lit(modulo)).cast("int"),
    )
    train_df = df_split.filter(F.col("split_key") != test_bucket)
    test_df = df_split.filter(F.col("split_key") == test_bucket)

    train_pdf = train_df.select(*feature_cols, "label").toPandas()
    test_pdf = test_df.select(*feature_cols, "label").toPandas()

    x_train = train_pdf[feature_cols]
    x_test = test_pdf[feature_cols]

    if task == "classification":
        y_train = train_pdf["label"].astype(str).str.strip()
        y_test = test_pdf["label"].astype(str).str.strip()
    else:
        y_train = pd.to_numeric(train_pdf["label"], errors="coerce")
        y_test = pd.to_numeric(test_pdf["label"], errors="coerce")

    return x_train, y_train, x_test, y_test


def train_classifier(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    max_depth: int,
    min_samples_leaf: int,
    ccp_alpha: float,
    random_state: int,
) -> DecisionTreeClassifier:
    model = DecisionTreeClassifier(
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        ccp_alpha=ccp_alpha,
        random_state=random_state,
        class_weight="balanced",
    )
    model.fit(x_train, y_train)
    return model


def train_regressor(
    x_train: pd.DataFrame,
    y_train: pd.Series,
    max_depth: int,
    min_samples_leaf: int,
    ccp_alpha: float,
    random_state: int,
) -> DecisionTreeRegressor:
    model = DecisionTreeRegressor(
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        ccp_alpha=ccp_alpha,
        random_state=random_state,
    )
    model.fit(x_train, y_train)
    return model


def evaluate_classifier(
    model: DecisionTreeClassifier, x_test: pd.DataFrame, y_test: pd.Series
) -> dict:
    y_pred = model.predict(x_test)
    return {
        "f1_macro": float(f1_score(y_test, y_pred, average="macro", zero_division=0)),
        "balanced_accuracy": float(balanced_accuracy_score(y_test, y_pred)),
    }


def evaluate_regressor(
    model: DecisionTreeRegressor, x_test: pd.DataFrame, y_test: pd.Series
) -> dict:
    y_pred = model.predict(x_test)
    mse = mean_squared_error(y_test, y_pred)
    return {
        "mae": float(mean_absolute_error(y_test, y_pred)),
        "rmse": float(math.sqrt(mse)),
        "r2": float(r2_score(y_test, y_pred)),
    }


def export_interpretability_artifacts(model, feature_cols: list[str], output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    importances = (
        pd.DataFrame({"feature": feature_cols, "importance": model.feature_importances_})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    importances.to_csv(os.path.join(output_dir, "feature_importances.csv"), index=False)

    rules_text = export_text(
        model,
        feature_names=list(feature_cols),
        max_depth=3,
        decimals=3,
        show_weights=True,
    )
    with open(os.path.join(output_dir, "top_splits_depth3.txt"), "w", encoding="utf-8") as fp:
        fp.write(rules_text)


def run_gold_ml_train_decision_tree(
    task: str = "classification",
    target_col: str = "target_score_extreme_droite_pct",
    max_depth: int = 5,
    min_samples_leaf: int = 20,
    ccp_alpha: float = 0.001,
    random_state: int = 42,
) -> None:
    spark = get_spark_session("Gold_ML_Train_DecisionTree")
    input_path = os.path.join(GOLD_PATH, "ml", "obt_ml_complete")
    df = spark.read.parquet(input_path)

    feature_cols = get_feature_columns(df.columns)
    if not feature_cols:
        raise ValueError("No feature column found with prefix 'feat_'.")

    if task == "classification":
        df_model = build_classification_frame(df, feature_cols)
    elif task == "regression":
        df_model = build_regression_frame(df, feature_cols, target_col)
    else:
        raise ValueError("task must be 'classification' or 'regression'.")

    x_train, y_train, x_test, y_test = split_by_bureau_group(
        df_model,
        feature_cols,
        task=task,
        test_bucket=4,
        modulo=5,
    )

    if task == "classification":
        model = train_classifier(
            x_train,
            y_train,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            ccp_alpha=ccp_alpha,
            random_state=random_state,
        )
        metrics = evaluate_classifier(model, x_test, y_test)
    else:
        model = train_regressor(
            x_train,
            y_train,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            ccp_alpha=ccp_alpha,
            random_state=random_state,
        )
        metrics = evaluate_regressor(model, x_test, y_test)

    output_dir = os.path.join(GOLD_PATH, "ml", "decision_tree_artifacts", task)
    export_interpretability_artifacts(model, feature_cols, output_dir)

    metrics_path = os.path.join(output_dir, "metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as fp:
        json.dump(metrics, fp, indent=2)

    logger.info("Decision tree training complete. Task=%s", task)
    logger.info("Metrics: %s", metrics)
    logger.info("Artifacts written to: %s", output_dir)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a decision tree model from Gold OBT ML table."
    )
    parser.add_argument(
        "--task", choices=["classification", "regression"], default="classification"
    )
    parser.add_argument("--target-col", default="target_score_extreme_droite_pct")
    parser.add_argument("--max-depth", type=int, default=5)
    parser.add_argument("--min-samples-leaf", type=int, default=20)
    parser.add_argument("--ccp-alpha", type=float, default=0.001)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_gold_ml_train_decision_tree(
        task=args.task,
        target_col=args.target_col,
        max_depth=args.max_depth,
        min_samples_leaf=args.min_samples_leaf,
        ccp_alpha=args.ccp_alpha,
        random_state=args.random_state,
    )
