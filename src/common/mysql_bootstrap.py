"""Utilities to seed the MySQL BI schema from raw CSV sources.

The project schema is created separately from the data load step. This module
populates the tables used by the notebook when MySQL is empty so the BI cells
can query real rows without requiring a manual ETL run first.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterable

import mysql.connector
import pandas as pd

from src.config import MAPPING_POLITIQUE_PATH, RAW_DATA_PATH, SECURITY_RAW_FILE

logger = logging.getLogger(__name__)


MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "lyon_decisional"),
}


def _to_int(value) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace(" ", "").replace(",", ".")
    try:
        return int(float(text))
    except ValueError:
        return None


def _to_float(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _native(value):
    if value is None:
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    return value


def _normalize_key(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip()).casefold()


def _derive_arrondissement(code_bureau: str | None) -> str | None:
    if not code_bureau:
        return None
    digits = re.sub(r"\D", "", str(code_bureau))
    if not digits:
        return None

    arrondissement_raw = _to_int(digits[:2])
    if arrondissement_raw in (0, 1):
        return "1er Arrondissement"
    if arrondissement_raw is None:
        return None
    return f"{arrondissement_raw}ème Arrondissement"


def _connect() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(**MYSQL_CONFIG)


def _table_row_count(cursor: mysql.connector.cursor.MySQLCursor, table_name: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return int(cursor.fetchone()[0])


def _insert_rows(
    cursor: mysql.connector.cursor.MySQLCursor, statement: str, rows: Iterable[tuple]
) -> None:
    for row in rows:
        cursor.execute(statement, row)


def _load_political_mapping() -> pd.DataFrame:
    if not os.path.exists(MAPPING_POLITIQUE_PATH):
        logger.warning("Political mapping file not found: %s", MAPPING_POLITIQUE_PATH)
        return pd.DataFrame()

    mapping = pd.read_csv(MAPPING_POLITIQUE_PATH, dtype=str).fillna("")
    mapping["nom_norm"] = mapping["nom"].map(_normalize_key)
    mapping["prenom_norm"] = mapping["prenom"].map(_normalize_key)
    return mapping


def _insert_year_dimension(
    cursor: mysql.connector.cursor.MySQLCursor, years: Iterable[int]
) -> None:
    rows = []
    for year in sorted({int(_native(year)) for year in years if _native(year) is not None}):
        rows.append((year, year, f"{year}-01-01", (year // 10) * 10))

    if not rows:
        return

    cursor.executemany(
        """
        INSERT IGNORE INTO dim_temps (sk_temps, annee, date_reference_annee, decennie)
        VALUES (%s, %s, %s, %s)
        """,
        rows,
    )


def _load_presidential_domain(cursor: mysql.connector.cursor.MySQLCursor) -> int:
    if _table_row_count(cursor, "fact_votes") > 0:
        return 0

    mapping = _load_political_mapping()
    mapping_lookup = {}
    for record in mapping.to_dict(orient="records"):
        mapping_lookup[(record["nom_norm"], record["prenom_norm"])] = record

    candidate_rows = {}
    bureau_rows = {}
    participation_rows = []
    fact_vote_rows = []
    years = {2022}

    presidential_files = [
        (1, os.path.join(RAW_DATA_PATH, "lyon_T1_presidentiel_2022.csv")),
        (2, os.path.join(RAW_DATA_PATH, "lyon_T2_presidentiel_2022.csv")),
    ]

    for tour, path in presidential_files:
        if not os.path.exists(path):
            logger.warning("Presidential raw file not found: %s", path)
            continue

        df = pd.read_csv(path, dtype=str).fillna("")
        for row in df.itertuples(index=False, name=None):
            bureau_id = str(row[6]).strip().zfill(4)
            arrondissement = _derive_arrondissement(bureau_id)
            inscrits = _to_int(row[7])
            abstentions = _to_int(row[8])
            votants = _to_int(row[10])
            exprimes = _to_int(row[18])

            bureau_rows[bureau_id] = (
                bureau_id,
                "69123",
                "Lyon",
                arrondissement,
                "Rattachement Administratif" if bureau_id == "0001" else "Standard",
            )

            if inscrits and votants is not None and exprimes is not None:
                participation_rows.append(
                    (
                        bureau_id,
                        tour,
                        inscrits,
                        abstentions,
                        votants,
                        exprimes,
                        round((votants / inscrits) * 100, 2) if inscrits else None,
                        round((abstentions / inscrits) * 100, 2) if inscrits else None,
                    )
                )

            candidate_start = 21
            candidate_fields = 7
            num_candidates = (len(row) - candidate_start) // candidate_fields
            for index in range(num_candidates):
                offset = candidate_start + index * candidate_fields
                sexe = str(row[offset + 1]).strip()
                nom = str(row[offset + 2]).strip()
                prenom = str(row[offset + 3]).strip()
                voix = _to_int(row[offset + 4])

                if not nom and not prenom:
                    continue

                candidate_key = (_normalize_key(nom), _normalize_key(prenom))
                mapped = mapping_lookup.get(candidate_key, {})
                candidate_id = (
                    mapped.get("id_candidat")
                    or f"{_normalize_key(nom)}|{_normalize_key(prenom)}|{_normalize_key(sexe)}"
                )

                candidate_rows[candidate_id] = (
                    candidate_id,
                    nom.strip().upper(),
                    prenom.strip().title(),
                    sexe,
                    mapped.get("parti_code") or None,
                    mapped.get("parti_nom") or None,
                    mapped.get("nuance_officielle") or None,
                    mapped.get("bloc_analytique") or None,
                )

                fact_vote_rows.append((bureau_id, candidate_id, tour, voix))

    _insert_year_dimension(cursor, years)

    if bureau_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO dim_geographie_bureau
            (id_bureau, code_insee, libelle_de_la_commune, arrondissement, type_bureau)
            VALUES (%s, %s, %s, %s, %s)
            """,
            list(bureau_rows.values()),
        )

    if candidate_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO dim_candidats
            (
                id_candidat, nom, prenom, sexe,
                parti_code, parti_nom, nuance_officielle, bloc_analytique
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            list(candidate_rows.values()),
        )

    if participation_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO fact_participation
            (
                id_bureau, tour, inscrits, abstentions,
                votants, exprimes, taux_participation, taux_abstention
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            participation_rows,
        )

    if fact_vote_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO fact_votes
            (id_bureau, id_candidat, tour, voix)
            VALUES (%s, %s, %s, %s)
            """,
            fact_vote_rows,
        )

    return len(fact_vote_rows)


def _load_security_domain(cursor: mysql.connector.cursor.MySQLCursor) -> int:
    if _table_row_count(cursor, "fact_securite") > 0:
        return 0

    if not os.path.exists(SECURITY_RAW_FILE):
        logger.warning("Security raw file not found: %s", SECURITY_RAW_FILE)
        return 0

    df = pd.read_csv(SECURITY_RAW_FILE, dtype=str).fillna("")
    if "CODGEO_2025" not in df.columns:
        logger.warning("Unexpected security CSV format; CODGEO_2025 column missing")
        return 0

    df = df[df["CODGEO_2025"].astype(str).str.match(r"^6938[1-9]$")]
    df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")
    df = df[df["annee"].between(2017, 2022)]
    df["nombre"] = pd.to_numeric(df["nombre"], errors="coerce").astype("Int64")
    df["taux_pour_mille"] = pd.to_numeric(df["taux_pour_mille"], errors="coerce")
    df["insee_pop"] = pd.to_numeric(df.get("insee_pop"), errors="coerce").astype("Int64")
    df["insee_log"] = pd.to_numeric(df.get("insee_log"), errors="coerce").astype("Int64")

    if df.empty:
        return 0

    _insert_year_dimension(cursor, df["annee"].dropna().astype(int).unique())

    geography_rows = []
    for code in sorted(df["CODGEO_2025"].dropna().astype(str).unique()):
        if code == "69381":
            name = "1er Arrondissement"
        elif code == "69382":
            name = "2ème Arrondissement"
        elif code == "69383":
            name = "3ème Arrondissement"
        elif code == "69384":
            name = "4ème Arrondissement"
        elif code == "69385":
            name = "5ème Arrondissement"
        elif code == "69386":
            name = "6ème Arrondissement"
        elif code == "69387":
            name = "7ème Arrondissement"
        elif code == "69388":
            name = "8ème Arrondissement"
        elif code == "69389":
            name = "9ème Arrondissement"
        else:
            name = "Inconnu"
        geography_rows.append((str(code), str(name)))

    indicator_rows = [
        (str(indicateur), str(unite))
        for indicateur, unite in df[["indicateur", "unite_de_compte"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    ]

    fact_rows = []
    for code, indicateur, annee, nombre, taux_pour_mille in (
        df[["CODGEO_2025", "indicateur", "annee", "nombre", "taux_pour_mille"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    ):
        fact_rows.append(
            (
                str(_native(code)),
                str(_native(indicateur)),
                int(_native(annee)) if _native(annee) is not None else None,
                int(_native(nombre)) if _native(nombre) is not None else None,
                float(_native(taux_pour_mille)) if _native(taux_pour_mille) is not None else None,
            )
        )

    demo_rows = []
    for code, annee, population, logements in (
        df[["CODGEO_2025", "annee", "insee_pop", "insee_log"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    ):
        demo_rows.append(
            (
                str(_native(code)),
                int(_native(annee)) if _native(annee) is not None else None,
                int(_native(population)) if _native(population) is not None else None,
                int(_native(logements)) if _native(logements) is not None else None,
            )
        )

    if geography_rows:
        _insert_rows(
            cursor,
            """
            INSERT IGNORE INTO dim_geographie_arrondissement
            (code_arrondissement, nom_arrondissement)
            VALUES (%s, %s)
            """,
            geography_rows,
        )

    if indicator_rows:
        _insert_rows(
            cursor,
            """
            INSERT IGNORE INTO dim_indicateurs_securite
            (id_indicateur, unite_de_compte)
            VALUES (%s, %s)
            """,
            indicator_rows,
        )

    if fact_rows:
        _insert_rows(
            cursor,
            """
            INSERT IGNORE INTO fact_securite
            (code_arrondissement, id_indicateur, annee, nombre, taux_pour_1000)
            VALUES (%s, %s, %s, %s, %s)
            """,
            fact_rows,
        )

    if demo_rows:
        _insert_rows(
            cursor,
            """
            INSERT IGNORE INTO fact_demographie_annuelle
            (code_arrondissement, annee, population, logements)
            VALUES (%s, %s, %s, %s)
            """,
            demo_rows,
        )

    return len(fact_rows)


def ensure_mysql_data_loaded() -> str:
    """Load MySQL tables from the raw CSV files if they are still empty."""

    connection = _connect()
    try:
        cursor = connection.cursor()
        loaded_votes = _load_presidential_domain(cursor)
        loaded_security = _load_security_domain(cursor)
        connection.commit()
        cursor.close()

        if loaded_votes or loaded_security:
            return (
                "Loaded raw presidential and security data into MySQL. "
                f"Votes inserted: {loaded_votes}, security rows inserted: {loaded_security}."
            )

        return "MySQL already contains data for the notebook tables."
    except mysql.connector.Error as exc:
        connection.rollback()
        logger.exception("Failed to seed MySQL data")
        return f"MySQL bootstrap failed: {exc}"
    finally:
        connection.close()
