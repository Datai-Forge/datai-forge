import logging
import os
import sys

import mysql.connector

# Add /app to sys.path
sys.path.append("/app")

from src.common.mysql_bootstrap import ensure_mysql_data_loaded
from src.database_loader import main as recreate_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "lyon_decisional"),
}


def drop_social_table():
    try:
        logger.info("Dropping fact_niveau_vie_pauvrete_200m to update schema...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS fact_niveau_vie_pauvrete_200m")
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Table dropped successfully.")
    except Exception as e:
        logger.error(f"Failed to drop table: {e}")
        sys.exit(1)


if __name__ == "__main__":
    drop_social_table()
    logger.info("Recreating schema...")
    recreate_schema()
    logger.info("Seeding data...")
    result = ensure_mysql_data_loaded()
    logger.info(result)
