"""Utilities to seed the MySQL BI schema from gold medallion outputs.

The project schema is created separately from this data load step. This module
populates BI tables used by notebooks when MySQL is empty.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterable

import mysql.connector
import pandas as pd

from src.config import GOLD_PATH

logger = logging.getLogger(__name__)


MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "lyon_decisional"),
}

GOLD_PRESIDENTIELLE_BI_PATH = os.path.join(GOLD_PATH, "presidentielle", "bi")
GOLD_SECURITE_BI_PATH = os.path.join(GOLD_PATH, "securite", "bi")
GOLD_NIVEAU_VIE_BI_STAR_PATH = os.path.join(GOLD_PATH, "niveau_vie_pauvrete", "bi_star")


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
    return f"{arrondissement_raw}eme Arrondissement"


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


def _read_gold_parquet(dataset_path: str, dataset_name: str) -> pd.DataFrame:
    if not os.path.exists(dataset_path):
        logger.warning("Gold dataset not found for %s: %s", dataset_name, dataset_path)
        return pd.DataFrame()

    try:
        return pd.read_parquet(dataset_path)
    except Exception:
        logger.exception("Failed to read gold dataset %s from %s", dataset_name, dataset_path)
        return pd.DataFrame()


def _records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return df.where(pd.notna(df), None).to_dict(orient="records")


def _to_date_string(value) -> str | None:
    native = _native(value)
    if native is None:
        return None
    parsed = pd.to_datetime(native, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _to_datetime_string(value) -> str | None:
    native = _native(value)
    if native is None:
        return None
    parsed = pd.to_datetime(native, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


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

    dim_candidats = _read_gold_parquet(
        os.path.join(GOLD_PRESIDENTIELLE_BI_PATH, "dim_candidats"),
        "presidentielle.dim_candidats",
    )
    dim_geographie = _read_gold_parquet(
        os.path.join(GOLD_PRESIDENTIELLE_BI_PATH, "dim_geographie"),
        "presidentielle.dim_geographie",
    )
    fact_participation = _read_gold_parquet(
        os.path.join(GOLD_PRESIDENTIELLE_BI_PATH, "fact_participation"),
        "presidentielle.fact_participation",
    )
    fact_votes = _read_gold_parquet(
        os.path.join(GOLD_PRESIDENTIELLE_BI_PATH, "fact_votes"),
        "presidentielle.fact_votes",
    )

    if fact_votes.empty:
        return 0

    _insert_year_dimension(cursor, [2022])

    candidate_rows = []
    for record in _records(dim_candidats):
        candidate_id = str(record.get("id_candidat") or "").strip()
        if not candidate_id:
            continue
        candidate_rows.append(
            (
                candidate_id,
                str(record.get("nom") or "").strip().upper() or None,
                str(record.get("prenom") or "").strip().title() or None,
                str(record.get("sexe") or "").strip() or None,
                str(record.get("parti_code") or "").strip() or None,
                str(record.get("parti_nom") or "").strip() or None,
                str(record.get("nuance_officielle") or "").strip() or None,
                str(record.get("bloc_analytique") or "").strip() or None,
            )
        )

    bureau_rows = []
    for record in _records(dim_geographie):
        bureau_id = str(record.get("id_bureau") or "").strip()
        if not bureau_id:
            continue
        bureau_rows.append(
            (
                bureau_id,
                str(record.get("code_insee") or "").strip() or "69123",
                str(record.get("libelle_de_la_commune") or "").strip() or "Lyon",
                str(record.get("arrondissement") or "").strip()
                or _derive_arrondissement(bureau_id),
                str(record.get("type_bureau") or "").strip()
                or ("Rattachement Administratif" if bureau_id == "0001" else "Standard"),
            )
        )

    participation_rows = []
    for record in _records(fact_participation):
        bureau_id = str(record.get("id_bureau") or "").strip()
        tour = _to_int(record.get("tour"))
        if not bureau_id or tour is None:
            continue
        participation_rows.append(
            (
                bureau_id,
                tour,
                _to_int(record.get("inscrits")),
                _to_int(record.get("abstentions")),
                _to_int(record.get("votants")),
                _to_int(record.get("exprimes")),
                _to_float(record.get("taux_participation")),
                _to_float(record.get("taux_abstention")),
            )
        )

    vote_rows = []
    for record in _records(fact_votes):
        bureau_id = str(record.get("id_bureau") or "").strip()
        candidate_id = str(record.get("id_candidat") or "").strip()
        tour = _to_int(record.get("tour"))
        if not bureau_id or not candidate_id or tour is None:
            continue
        vote_rows.append((bureau_id, candidate_id, tour, _to_int(record.get("voix"))))

    if bureau_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO dim_geographie_bureau
            (id_bureau, code_insee, libelle_de_la_commune, arrondissement, type_bureau)
            VALUES (%s, %s, %s, %s, %s)
            """,
            bureau_rows,
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
            candidate_rows,
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

    if vote_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO fact_votes
            (id_bureau, id_candidat, tour, voix)
            VALUES (%s, %s, %s, %s)
            """,
            vote_rows,
        )

    return len(vote_rows)


def _load_security_domain(cursor: mysql.connector.cursor.MySQLCursor) -> int:
    if _table_row_count(cursor, "fact_securite") > 0:
        return 0

    dim_geographie = _read_gold_parquet(
        os.path.join(GOLD_SECURITE_BI_PATH, "dim_geographie_lyon"),
        "securite.dim_geographie_lyon",
    )
    dim_indicateurs = _read_gold_parquet(
        os.path.join(GOLD_SECURITE_BI_PATH, "dim_indicateurs_securite"),
        "securite.dim_indicateurs_securite",
    )
    fact_securite = _read_gold_parquet(
        os.path.join(GOLD_SECURITE_BI_PATH, "fact_securite"),
        "securite.fact_securite",
    )
    fact_demographie = _read_gold_parquet(
        os.path.join(GOLD_SECURITE_BI_PATH, "fact_demographie_annuelle"),
        "securite.fact_demographie_annuelle",
    )

    if fact_securite.empty:
        return 0

    geography_rows = []
    for record in _records(dim_geographie):
        code = str(record.get("code_arrondissement") or "").strip()
        if not code:
            continue
        geography_rows.append(
            (code, str(record.get("nom_arrondissement") or "").strip() or "Inconnu")
        )

    indicator_rows = []
    for record in _records(dim_indicateurs):
        indicator_id = str(record.get("id_indicateur") or "").strip()
        if not indicator_id:
            continue
        indicator_rows.append(
            (indicator_id, str(record.get("unite_de_compte") or "").strip() or None)
        )

    securite_rows = []
    for record in _records(fact_securite):
        code = str(record.get("code_arrondissement") or "").strip()
        indicator_id = str(record.get("id_indicateur") or "").strip()
        annee = _to_int(record.get("annee"))
        if not code or not indicator_id or annee is None:
            continue
        securite_rows.append(
            (
                code,
                indicator_id,
                annee,
                _to_int(record.get("nombre")),
                _to_float(record.get("taux_pour_1000")),
            )
        )

    demo_rows = []
    for record in _records(fact_demographie):
        code = str(record.get("code_arrondissement") or "").strip()
        annee = _to_int(record.get("annee"))
        if not code or annee is None:
            continue
        demo_rows.append(
            (
                code,
                annee,
                _to_int(record.get("population")),
                _to_int(record.get("logements")),
            )
        )

    if securite_rows:
        _insert_year_dimension(cursor, [row[2] for row in securite_rows])

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

    if securite_rows:
        _insert_rows(
            cursor,
            """
            INSERT IGNORE INTO fact_securite
            (code_arrondissement, id_indicateur, annee, nombre, taux_pour_1000)
            VALUES (%s, %s, %s, %s, %s)
            """,
            securite_rows,
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

    return len(securite_rows)


def _load_poverty_domain(cursor: mysql.connector.cursor.MySQLCursor) -> int:
    if _table_row_count(cursor, "fact_niveau_vie_pauvrete_200m") > 0:
        return 0

    dim_geographie = _read_gold_parquet(
        os.path.join(GOLD_NIVEAU_VIE_BI_STAR_PATH, "dim_geographie_200m"),
        "niveau_vie_pauvrete.dim_geographie_200m",
    )
    dim_temps = _read_gold_parquet(
        os.path.join(GOLD_NIVEAU_VIE_BI_STAR_PATH, "dim_temps"),
        "niveau_vie_pauvrete.dim_temps",
    )
    fact = _read_gold_parquet(
        os.path.join(GOLD_NIVEAU_VIE_BI_STAR_PATH, "fact_niveau_vie_pauvrete_200m"),
        "niveau_vie_pauvrete.fact_niveau_vie_pauvrete_200m",
    )

    if fact.empty:
        return 0

    geography_rows = []
    for record in _records(dim_geographie):
        sk_geographie = _to_int(record.get("sk_geographie"))
        if sk_geographie is None:
            continue
        geography_rows.append(
            (
                sk_geographie,
                record.get("identifiant_carreaux_au_200m"),
                record.get("id_carreaux_au_1km"),
                record.get("id_inspire_carreau_nature_dedie_au_carreau_200_m"),
                record.get("identifiant_est_200m"),
                record.get("id_est_au_1km"),
                record.get("arrondissement"),
                record.get("commune"),
                record.get("code_commune"),
                record.get("lcog_geo_2"),
                record.get("lcog_geo_3"),
                record.get("lcog_geo_4"),
                record.get("lcog_geo_5"),
            )
        )

    time_rows = []
    for record in _records(dim_temps):
        sk_temps = _to_int(record.get("sk_temps"))
        annee = _to_int(record.get("annee"))
        if sk_temps is None or annee is None:
            continue
        time_rows.append(
            (
                sk_temps,
                annee,
                _to_date_string(record.get("date_reference_annee")),
                _to_int(record.get("decennie")),
            )
        )

    fact_rows = []
    for record in _records(fact):
        sk_geographie = _to_int(record.get("sk_geographie"))
        sk_temps = _to_int(record.get("sk_temps"))
        if sk_geographie is None or sk_temps is None:
            continue
        fact_rows.append(
            (
                sk_geographie,
                sk_temps,
                _to_int(record.get("nb_individus")),
                _to_int(record.get("nb_menages")),
                _to_float(record.get("somme_niveaux_de_vie_winsorises_des_individus")),
                _to_int(record.get("nb_individus_18-24_ans")),
                _to_int(record.get("nb_individus_de_25-39_ans")),
                _to_int(record.get("nb_individus_40-54_ans")),
                _to_int(record.get("nb_individus_55-64_ans")),
                _to_int(record.get("nb_individus_65-79_ans")),
                _to_int(record.get("nb_individus_+80_ans")),
                _to_datetime_string(record.get("gold_processing_timestamp")),
            )
        )

    if time_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO dim_temps (sk_temps, annee, date_reference_annee, decennie)
            VALUES (%s, %s, %s, %s)
            """,
            time_rows,
        )

    if geography_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO dim_geographie_200m (
                sk_geographie,
                identifiant_carreaux_au_200m,
                id_carreaux_au_1km,
                id_inspire_carreau_nature_dedie_au_carreau_200_m,
                identifiant_est_200m,
                id_est_au_1km,
                arrondissement,
                commune,
                code_commune,
                lcog_geo_2,
                lcog_geo_3,
                lcog_geo_4,
                lcog_geo_5
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            geography_rows,
        )

    if fact_rows:
        cursor.executemany(
            """
            INSERT IGNORE INTO fact_niveau_vie_pauvrete_200m
            (
                sk_geographie, sk_temps, nb_individus, nb_menages,
                somme_niveaux_de_vie_winsorises_des_individus,
                nb_individus_18_24_ans, nb_individus_25_39_ans, nb_individus_40_54_ans,
                nb_individus_55_64_ans, nb_individus_65_79_ans, nb_individus_80p_ans,
                gold_processing_timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            fact_rows,
        )

    return len(fact_rows)


def ensure_mysql_data_loaded() -> str:
    """Load MySQL tables from gold medallion datasets if they are still empty."""

    connection = _connect()
    try:
        cursor = connection.cursor()
        loaded_poverty = _load_poverty_domain(cursor)
        loaded_votes = _load_presidential_domain(cursor)
        loaded_security = _load_security_domain(cursor)
        connection.commit()
        cursor.close()

        if loaded_poverty or loaded_votes or loaded_security:
            return (
                "Loaded gold medallion datasets into MySQL. "
                f"Poverty rows inserted: {loaded_poverty}, "
                f"votes inserted: {loaded_votes}, security rows inserted: {loaded_security}."
            )

        return "MySQL already contains data for the notebook tables."
    except mysql.connector.Error as exc:
        connection.rollback()
        logger.exception("Failed to seed MySQL data")
        return f"MySQL bootstrap failed: {exc}"
    finally:
        connection.close()
