import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from src.common.spark_session_manager import get_spark_session
from src.config import GOLD_PATH
from src.ml.model_factory import ModelFactory


def generate_ml_visualizations():
    # Chargement des données (OBT)
    spark = get_spark_session("ML_Visualization")
    obt_path = os.path.join(GOLD_PATH, "ml", "obt_ml_complete")
    df = spark.read.parquet(obt_path).toPandas()

    # Préparation pour le Tour 1 - Bloc Gauche
    target = "target_score_gauche_pct"
    df_t1 = df[df["tour"] == 1].copy()
    features = [c for c in df.columns if c.startswith("feat_")]

    X = df_t1[features]
    y = df_t1[target]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # --- ÉTAPE CRUCIALE : STANDARDISATION ---
    # On centre et réduit les données pour que les coefficients soient comparables
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    plot_dir = "reports/ml/plots"
    os.makedirs(plot_dir, exist_ok=True)

    # On compare les modèles
    models = ["linear", "rf"]

    for m_name in models:
        model = ModelFactory.get_model(m_name)

        # On entraîne sur les données SCALÉES (obligatoire pour l'interprétation linéaire)
        model.fit(X_train_scaled, y_train)
        y_pred = model.predict(X_test_scaled)

        r2 = r2_score(y_test, y_pred)
        residuals = y_test - y_pred

        # --- GRAPH 1: Prédiction vs Réalité ---
        plt.figure(figsize=(10, 6))
        plt.scatter(y_test, y_pred, alpha=0.6, color="blue" if m_name == "linear" else "green")
        plt.plot([y.min(), y.max()], [y.min(), y.max()], "r--", lw=2)
        plt.xlabel("Score Réel (%)")
        plt.ylabel("Score Prédit (%)")
        plt.title(f"Prédiction vs Réalité - {m_name.upper()} (R² = {r2:.4f})")
        plt.grid(True)
        plt.savefig(f"{plot_dir}/{m_name}_pred_vs_real.png")
        plt.close()

        # --- GRAPH 2: Graphique des Résidus ---
        plt.figure(figsize=(10, 6))
        plt.scatter(y_pred, residuals, alpha=0.6, color="orange")
        plt.axhline(y=0, color="r", linestyle="--")
        plt.xlabel("Score Prédit (%)")
        plt.ylabel("Résidus (Erreur)")
        plt.title(f"Graphique des Résidus - {m_name.upper()}")
        plt.grid(True)
        plt.savefig(f"{plot_dir}/{m_name}_residuals.png")
        plt.close()

        # --- GRAPH 5: Coefficients (Linear uniquement) ---
        if m_name == "linear":
            coefs = pd.Series(model.coef_, index=features).sort_values()
            plt.figure(figsize=(12, 8))
            colors = ["red" if x < 0 else "blue" for x in coefs.values]
            coefs.plot(kind="barh", color=colors)
            plt.title(
                "Coeff (Régression Linéaire)\nInterprétation : Influence + (bleu) ou - (rouge)"
            )
            plt.xlabel("Poids du coefficient (standardisé)")
            plt.axvline(x=0, color="black", linestyle="-", linewidth=1)
            plt.tight_layout()
            plt.savefig(f"{plot_dir}/linear_coefficients.png")
            plt.close()

    # --- GRAPH 3: Feature Importance (Random Forest uniquement) ---
    model_rf = ModelFactory.get_model("rf")
    # RF n'a pas besoin de scaling mais on garde X_train_scaled pour la cohérence
    model_rf.fit(X_train_scaled, y_train)
    importances = pd.Series(model_rf.feature_importances_, index=features).sort_values(
        ascending=False
    )

    plt.figure(figsize=(12, 8))
    importances.head(10).plot(kind="barh", color="skyblue")
    plt.title("Top 10 Importance des Variables (Random Forest)")
    plt.xlabel("Importance Relative (toujours positif)")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(f"{plot_dir}/rf_feature_importance.png")
    plt.close()

    # LE "KILLER PLOT" (Granularité) ---
    plt.figure(figsize=(12, 8))
    sns.scatterplot(
        data=df_t1,
        x="feat_social_taux_pauvrete",
        y=target,
        hue="code_insee_arrondissement",
        palette="viridis",
        s=100,
    )
    plt.title("Preuve du Biais de Granularité : Vote vs Pauvreté (par Arrondissement)")
    plt.xlabel("Taux de Pauvreté de l'Arrondissement (%)")
    plt.ylabel("Score du Bloc Gauche dans le Bureau de Vote (%)")
    plt.legend(title="Arrondissement", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{plot_dir}/granularity_bias_check.png")
    plt.close()

    print(f"--- Graphiques mis à jour avec Standardisation dans : {plot_dir} ---")
    spark.stop()


if __name__ == "__main__":
    generate_ml_visualizations()
