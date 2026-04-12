"""
Database schema initialization script for Lyon Decisional System.

This script creates the relational database schema from Gold layer outputs.
It is designed to run AFTER both MySQL and the app container are fully ready.

Usage:
    python3 -m src.database_loader

Environment Variables (optional, with safe defaults):
    MYSQL_HOST       : MySQL hostname (default: mysql)
    MYSQL_PORT       : MySQL port (default: 3306)
    MYSQL_USER       : MySQL user (default: root)
    MYSQL_PASSWORD   : MySQL password (default: empty)
    MYSQL_DATABASE   : Target database (default: lyon_decisional)
    MAX_RETRIES      : Number of connection attempts (default: 5)
    RETRY_DELAY      : Seconds between retries (default: 2)
"""

import logging
import os
import sys
import time

try:
    import mysql.connector
    from mysql.connector import Error, ProgrammingError
except ImportError:
    print("ERROR: mysql-connector-python not installed. Install with:")
    print("  pip install mysql-connector-python")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Database configuration with safe defaults
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "lyon_decisional"),
    "autocommit": False,
}

# Log configuration (with masked password)
logger.info("MySQL Configuration:")
logger.info(f"  Host: {DB_CONFIG['host']}")
logger.info(f"  Port: {DB_CONFIG['port']}")
logger.info(f"  User: {DB_CONFIG['user']}")
logger.info(
    f"  Password: {'*' * len(DB_CONFIG['password']) if DB_CONFIG['password'] else '(empty)'}"
)
logger.info(f"  Database: {DB_CONFIG['database']}")
logger.info(
    "  Environment variables present: "
    f"MYSQL_HOST={bool(os.getenv('MYSQL_HOST'))}, "
    f"MYSQL_USER={bool(os.getenv('MYSQL_USER'))}, "
    f"MYSQL_PASSWORD={bool(os.getenv('MYSQL_PASSWORD'))}"
)

# Retry configuration
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "2"))

# SQL schema definitions
SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS dim_temps (
        sk_temps INT PRIMARY KEY,
        annee INT NOT NULL UNIQUE,
        date_reference_annee DATE,
        decennie INT
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_geographie_200m (
        sk_geographie BIGINT PRIMARY KEY,
        identifiant_carreaux_au_200m TEXT,
        id_carreaux_au_1km TEXT,
        id_inspire_carreau_nature_dedie_au_carreau_200_m TEXT,
        identifiant_est_200m TEXT,
        id_est_au_1km TEXT,
        arrondissement TEXT,
        commune TEXT,
        code_commune TEXT,
        lcog_geo_2 TEXT,
        lcog_geo_3 TEXT,
        lcog_geo_4 TEXT,
        lcog_geo_5 TEXT
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_niveau_vie_pauvrete_200m (
        sk_geographie BIGINT NOT NULL,
        sk_temps INT NOT NULL,
        nb_individus INT,
        nb_menages INT,
        somme_niveaux_de_vie_winsorises_des_individus DOUBLE,
        nb_individus_18_24_ans INT,
        nb_individus_25_39_ans INT,
        nb_individus_40_54_ans INT,
        nb_individus_55_64_ans INT,
        nb_individus_65_79_ans INT,
        nb_individus_80p_ans INT,
        gold_processing_timestamp DATETIME,
        PRIMARY KEY (sk_geographie, sk_temps),
        CONSTRAINT fk_nvp_geo
            FOREIGN KEY (sk_geographie) REFERENCES dim_geographie_200m(sk_geographie),
        CONSTRAINT fk_nvp_temps
            FOREIGN KEY (sk_temps) REFERENCES dim_temps(sk_temps)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_candidats (
        id_candidat VARCHAR(255) PRIMARY KEY,
        nom TEXT,
        prenom TEXT,
        sexe TEXT,
        parti_code TEXT,
        parti_nom TEXT,
        nuance_officielle TEXT,
        bloc_analytique TEXT
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_geographie_bureau (
        id_bureau VARCHAR(255) PRIMARY KEY,
        code_insee VARCHAR(10) NOT NULL,
        libelle_de_la_commune TEXT,
        arrondissement TEXT,
        type_bureau TEXT
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_votes (
        id_bureau VARCHAR(255) NOT NULL,
        id_candidat VARCHAR(255) NOT NULL,
        tour INT NOT NULL,
        voix INT,
        PRIMARY KEY (id_bureau, id_candidat, tour),
        CONSTRAINT fk_votes_bureau
            FOREIGN KEY (id_bureau) REFERENCES dim_geographie_bureau(id_bureau),
        CONSTRAINT fk_votes_candidat
            FOREIGN KEY (id_candidat) REFERENCES dim_candidats(id_candidat)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_participation (
        id_bureau VARCHAR(255) NOT NULL,
        tour INT NOT NULL,
        inscrits INT,
        abstentions INT,
        votants INT,
        exprimes INT,
        taux_participation DECIMAL(5,2),
        taux_abstention DECIMAL(5,2),
        PRIMARY KEY (id_bureau, tour),
        CONSTRAINT fk_participation_bureau
            FOREIGN KEY (id_bureau) REFERENCES dim_geographie_bureau(id_bureau)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_indicateurs_securite (
        id_indicateur VARCHAR(255) PRIMARY KEY,
        unite_de_compte TEXT
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_geographie_arrondissement (
        code_arrondissement VARCHAR(10) PRIMARY KEY,
        nom_arrondissement TEXT
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_securite (
        code_arrondissement VARCHAR(10) NOT NULL,
        id_indicateur VARCHAR(255) NOT NULL,
        annee INT NOT NULL,
        nombre INT,
        taux_pour_1000 DECIMAL(10,4),
        PRIMARY KEY (code_arrondissement, id_indicateur, annee),
        CONSTRAINT fk_securite_geo
            FOREIGN KEY (code_arrondissement)
                REFERENCES dim_geographie_arrondissement(code_arrondissement),
        CONSTRAINT fk_securite_indicateur
            FOREIGN KEY (id_indicateur) REFERENCES dim_indicateurs_securite(id_indicateur)
    ) ENGINE=InnoDB;
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_demographie_annuelle (
        code_arrondissement VARCHAR(10) NOT NULL,
        annee INT NOT NULL,
        population INT,
        logements INT,
        PRIMARY KEY (code_arrondissement, annee),
        CONSTRAINT fk_demo_geo
            FOREIGN KEY (code_arrondissement)
                REFERENCES dim_geographie_arrondissement(code_arrondissement)
    ) ENGINE=InnoDB;
    """,
]


def connect_with_retry() -> mysql.connector.MySQLConnection | None:
    """
    Attempt to connect to MySQL with exponential backoff retry logic.

    Returns:
        MySQL connection object on success, None on failure after all retries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                f"Connection attempt {attempt}/{MAX_RETRIES} to "
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}..."
            )
            conn = mysql.connector.connect(**DB_CONFIG)
            logger.info("✅ Successfully connected to MySQL.")
            return conn
        except Error as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"❌ Connection failed: {e}. " f"Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(
                    f"❌ Failed to connect after {MAX_RETRIES} attempts. " f"Last error: {e}"
                )
                return None


def create_database_if_not_exists(conn: mysql.connector.MySQLConnection) -> bool:
    """
    Create the target database if it does not exist.

    Args:
        conn: Active MySQL connection (connected to MySQL server, not specific DB).

    Returns:
        True on success, False on failure.
    """
    cursor = conn.cursor()
    try:
        db_name = DB_CONFIG["database"]
        logger.info(f"Ensuring database '{db_name}' exists...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.commit()
        logger.info(f"✅ Database '{db_name}' ready.")
        return True
    except ProgrammingError as e:
        logger.error(f"❌ Failed to create database: {e}")
        return False
    finally:
        cursor.close()


def create_tables(conn: mysql.connector.MySQLConnection) -> bool:
    """
    Execute all schema creation statements in order.

    Args:
        conn: Active MySQL connection (must be connected to target database).

    Returns:
        True on success, False on failure.
    """
    cursor = conn.cursor()
    try:
        for i, statement in enumerate(SCHEMA_SQL, 1):
            logger.info(f"Creating table {i}/{len(SCHEMA_SQL)}...")
            cursor.execute(statement)
        conn.commit()
        logger.info(f"✅ All {len(SCHEMA_SQL)} tables created successfully.")
        return True
    except ProgrammingError as e:
        logger.error(f"❌ SQL execution failed: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()


def main() -> int:
    """
    Main entry point: connect, create database, create schema.

    Returns:
        0 on success, 1 on failure.
    """
    logger.info("=" * 70)
    logger.info("Lyon Decisional Database Schema Initialization")
    logger.info("=" * 70)
    logger.info(f"Target: {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
    logger.info(f"Database: {DB_CONFIG['database']}")
    logger.info("=" * 70)

    # Step 1: Connect to MySQL (with retries)
    conn = connect_with_retry()
    if not conn:
        logger.error("Cannot proceed without MySQL connection.")
        return 1

    # Step 2: Create database if needed (requires server-level connection)
    if not create_database_if_not_exists(conn):
        conn.close()
        return 1

    # Step 3: Switch to target database and create schema
    conn.database = DB_CONFIG["database"]
    if not create_tables(conn):
        conn.close()
        return 1

    conn.close()
    logger.info("=" * 70)
    logger.info("✅ Database schema initialization complete!")
    logger.info("=" * 70)
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
