import os

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH
from src.ml.model_factory import ModelFactory


def run_benchmark():
    # Chargement des données (OBT)
    spark = get_spark_session("ML_Benchmark")
    obt_path = os.path.join(GOLD_PATH, "ml", "obt_ml_complete")

    print(f"--- Chargement de l'OBT : {obt_path} ---")
    df = spark.read.parquet(obt_path).toPandas()

    # Préparation pour le Tour 1 - Bloc Gauche
    target = "target_score_gauche_pct"
    df_t1 = df[df["tour"] == 1].copy()

    # Sélection des features (tous les feat_*)
    features = [c for c in df.columns if c.startswith("feat_")]

    X = df_t1[features]
    y = df_t1[target]

    # Split Train/Test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print(f"Dataset : {X.shape[0]} lignes | Features : {len(features)}")
    print(f"Train size : {X_train.shape[0]} | Test size : {X_test.shape[0]}")

    # Comparaison des modèles
    models_to_test = ["linear", "rf"]
    results = []

    for m_name in models_to_test:
        print(f"\n--- Entraînement du modèle : {m_name} ---")
        model = ModelFactory.get_model(m_name)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        # Métriques
        r2 = r2_score(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae = mean_absolute_error(y_test, y_pred)

        results.append({"model": m_name, "R2": r2, "RMSE": rmse, "MAE": mae})

        print(f"RESULTAT {m_name.upper()} : R2 = {r2:.4f} | RMSE = {rmse:.4f}%")

        # Importance des features pour le RF
        if m_name == "rf":
            importances = pd.Series(model.feature_importances_, index=features).sort_values(
                ascending=False
            )
            print("\nTop 5 Features Importantes (Random Forest) :")
            print(importances.head(5))

    # 5. Conclusion
    df_results = pd.DataFrame(results)
    print("\n--- SYNTHESE COMPARATIVE ---")
    print(df_results.sort_values(by="R2", ascending=False))

    spark.stop()


if __name__ == "__main__":
    run_benchmark()
