import os
import logging

# Configuration minimaliste pour le diagnostic de la config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("config")

# Ici on centralise tous les chemins du projet.

# Chemin racine du projet (on se base sur l'emplacement de ce fichier config.py)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logger.info(f"Racine du projet : {PROJECT_ROOT}")

# Dossiers de sortie des données
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data-raw")
BRONZE_PATH = os.path.join(PROJECT_ROOT, "bronze")
SILVER_PATH = os.path.join(PROJECT_ROOT, "silver")
GOLD_PATH = os.path.join(PROJECT_ROOT, "gold")

# Configuration spécifique aux élections
PRESIDENTIELLE_BRONZE_PATH = os.path.join(BRONZE_PATH, "presidentielle")

# On s'assure que les dossiers existent à l'import de la config
for path in [BRONZE_PATH, SILVER_PATH, GOLD_PATH, PRESIDENTIELLE_BRONZE_PATH]:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logger.info(f"Création du dossier de données : {path}")
    else:
        logger.debug(f"Dossier déjà présent : {path}")
