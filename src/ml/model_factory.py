from sklearn.base import BaseEstimator
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression


class ModelFactory:
    """Usine à modèles pour centraliser la configuration des estimateurs ML."""

    @staticmethod
    def get_model(model_name: str, params: dict = None) -> BaseEstimator:
        """Retourne une instance de modèle configurée."""
        params = params or {}

        if model_name == "linear":
            return LinearRegression(**params)
        elif model_name == "rf":
            # RandomForest est robuste à la multicolinéarité
            # On fixe random_state pour la reproductibilité
            params.setdefault("n_estimators", 100)
            params.setdefault("random_state", 42)
            params.setdefault("n_jobs", -1)
            return RandomForestRegressor(**params)
        else:
            raise ValueError(f"Modèle '{model_name}' non reconnu. Choisissez 'linear' ou 'rf'.")
