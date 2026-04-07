import os

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH
from src.ml.model_factory import ModelFactory


def run_global_analysis():
    # Chargement des données (OBT)
    spark = get_spark_session("Full_Election_Analysis")
    obt_path = os.path.join(GOLD_PATH, "ml", "obt_ml_complete")
    df = spark.read.parquet(obt_path).toPandas()

    # Préparation globale (Tour 1)
    df_t1 = df[df["tour"] == 1].copy()
    features = [c for c in df.columns if c.startswith("feat_")]
    targets = [c for c in df.columns if c.startswith("target_")]

    # Split Train/Test commun (pour la comparabilité)
    # On splitte sur l'ID bureau pour que tous les modèles voient les mêmes BV
    train_indices, test_indices = train_test_split(df_t1.index, test_size=0.2, random_state=42)

    X_train = df_t1.loc[train_indices, features]
    X_test = df_t1.loc[test_indices, features]

    # Standardisation commune
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    full_results = []

    print("\n--- ANALYSE GLOBALE DE L'ÉLECTION (Tour 1) ---")
    print(f"Dataset : {len(df_t1)} Bureaux de Vote | Features : {len(features)}")

    for t in targets:
        bloc_name = t.replace("target_score_", "").replace("_pct", "").upper()
        print(f"\n🚀 Modélisation du bloc : {bloc_name}")

        y_train = df_t1.loc[train_indices, t]
        y_test = df_t1.loc[test_indices, t]

        # Test de la Régression Linéaire (Baseline)
        model = ModelFactory.get_model("linear")
        model.fit(X_train_scaled, y_train)
        y_pred = model.predict(X_test_scaled)

        from sklearn.metrics import mean_absolute_error

        r2 = r2_score(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae = mean_absolute_error(y_test, y_pred)

        full_results.append({"Bloc": bloc_name, "R2": r2, "RMSE (%)": rmse, "MAE (%)": mae})

        print(f"   R² = {r2:.4f} | RMSE = {rmse:.2f}% | MAE = {mae:.2f}%")

    # Synthèse
    df_summary = pd.DataFrame(full_results).sort_values(by="R2", ascending=False)
    print("\n" + "=" * 50)
    print("📊 RAPPORT FINAL DE PRÉDICTION (BASELINE)")
    print("=" * 50)
    print(df_summary.to_string(index=False))
    print("=" * 50)

    # Sauvegarde du rapport pour documentation
    report_path = "reports/ml/global_performance_report.csv"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    df_summary.to_csv(report_path, index=False)
    print(f"\nRapport sauvegardé dans : {report_path}")

    spark.stop()


if __name__ == "__main__":
    run_global_analysis()
