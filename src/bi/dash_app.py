"""Dash BI application for election, social and security indicators."""

from __future__ import annotations

import os
import re
import socket
from pathlib import Path

import dash
import mysql.connector
import pandas as pd
import plotly.express as px
from dash import Dash, dash_table, dcc, html

from src.common.mysql_bootstrap import ensure_mysql_data_loaded


def get_connection() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "lyon_decisional"),
    )


def read_sql(sql: str, params: list | None = None) -> pd.DataFrame:
    connection = get_connection()
    try:
        return pd.read_sql_query(sql, connection, params=params)
    finally:
        connection.close()


def pick_nvp_metric() -> str | None:
    columns = read_sql(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'fact_niveau_vie_pauvrete_200m'
        ORDER BY ordinal_position
        """
    )
    columns.columns = [c.lower() for c in columns.columns]
    if "column_name" not in columns.columns:
        return None

    excluded_cols = {"sk_geographie", "sk_temps", "gold_processing_timestamp"}
    measure_candidates = [c for c in columns["column_name"].tolist() if c not in excluded_cols]
    if not measure_candidates:
        return None

    priority_patterns = ["niveau_vie", "med", "revenu", "pauvrete", "taux"]
    for pattern in priority_patterns:
        match = next((m for m in measure_candidates if pattern in m.lower()), None)
        if match is not None:
            return match

    return measure_candidates[0]


def normalize_arrondissement(value: object) -> str | None:
    match = re.search(r"([1-9])", str(value))
    return match.group(1) if match else None


def load_nvp_by_arrondissement() -> tuple[pd.DataFrame, str, int]:
    selected_metric = pick_nvp_metric()
    if selected_metric is not None:
        sql_df = read_sql(
            f"""
            SELECT
                g.arrondissement AS arrondissement,
                t.annee,
                AVG(f.`{selected_metric}`) AS niveau_vie_moyen
            FROM fact_niveau_vie_pauvrete_200m f
            JOIN dim_geographie_200m g ON g.sk_geographie = f.sk_geographie
            JOIN dim_temps t ON t.sk_temps = f.sk_temps
            WHERE g.arrondissement IS NOT NULL
              AND f.`{selected_metric}` IS NOT NULL
            GROUP BY g.arrondissement, t.annee
            ORDER BY t.annee, g.arrondissement
            """
        )
        if not sql_df.empty:
            latest_year = int(sql_df["annee"].max())
            return sql_df, selected_metric, latest_year

    fallback_candidates = [
        Path("data-raw/2021_carreaux_200m_met.csv"),
        Path("../data-raw/2021_carreaux_200m_met.csv"),
        Path("../../data-raw/2021_carreaux_200m_met.csv"),
        Path("/app/data-raw/2021_carreaux_200m_met.csv"),
    ]
    fallback_path = next((p for p in fallback_candidates if p.exists()), None)
    if fallback_path is None:
        return (
            pd.DataFrame(columns=["arrondissement", "annee", "niveau_vie_moyen"]),
            "ind_snv",
            2021,
        )

    raw = pd.read_csv(fallback_path)
    fallback_metric = next((c for c in ["ind_snv", "men_pauv"] if c in raw.columns), None)
    if fallback_metric is None or "lcog_geo" not in raw.columns:
        return (
            pd.DataFrame(columns=["arrondissement", "annee", "niveau_vie_moyen"]),
            "ind_snv",
            2021,
        )

    raw["arrondissement"] = (
        raw["lcog_geo"]
        .astype(str)
        .str.replace('"', "", regex=False)
        .str.extract(r"(6938[1-9])", expand=False)
        .str[-1]
    )
    out = (
        raw.dropna(subset=["arrondissement", fallback_metric])
        .groupby("arrondissement", as_index=False)[fallback_metric]
        .mean()
        .rename(columns={fallback_metric: "niveau_vie_moyen"})
    )
    out["annee"] = 2021
    return out, fallback_metric, 2021


def load_participation_by_bureau() -> pd.DataFrame:
    return read_sql(
        """
        SELECT
            b.id_bureau,
            b.arrondissement,
            p.tour,
            p.inscrits,
            p.taux_participation
        FROM fact_participation p
        JOIN dim_geographie_bureau b ON b.id_bureau = p.id_bureau
        WHERE b.arrondissement IS NOT NULL
        """
    )


def build_scatter_figure(data: pd.DataFrame, metric_label: str, year: int):
    if data.empty:
        return px.scatter(title="Aucune donnee disponible pour niveau de vie vs participation")
    fig = px.scatter(
        data,
        x="niveau_vie_moyen",
        y="taux_participation",
        color="tour",
        size="inscrits",
        hover_data=["id_bureau", "arrondissement"],
        title=f"Niveau de vie ({year}) vs participation par bureau",
        labels={
            "niveau_vie_moyen": f"Niveau de vie moyen arrondissement ({metric_label})",
            "taux_participation": "Taux de participation (%)",
            "tour": "Tour",
        },
    )
    fig.update_layout(legend_title_text="Tour")
    return fig


def build_top_bureaux_figure(data: pd.DataFrame):
    if data.empty:
        return px.bar(title="Aucune donnee disponible pour les bureaux")
    top_bureaux = data.sort_values("taux_participation", ascending=False).head(15).copy()
    top_bureaux["id_bureau"] = top_bureaux["id_bureau"].astype(str)
    fig = px.bar(
        top_bureaux.sort_values("taux_participation"),
        x="taux_participation",
        y="id_bureau",
        color="tour",
        orientation="h",
        title="Top 15 bureaux par taux de participation",
        labels={
            "id_bureau": "Bureau de vote",
            "taux_participation": "Taux de participation (%)",
            "tour": "Tour",
        },
    )
    fig.update_layout(legend_title_text="Tour")
    return fig


def load_demography_security_trends() -> pd.DataFrame:
    demography = read_sql(
        """
        SELECT
            g.nom_arrondissement AS arrondissement,
            d.annee,
            d.population
        FROM fact_demographie_annuelle d
        JOIN dim_geographie_arrondissement g ON g.code_arrondissement = d.code_arrondissement
        """
    )
    security = read_sql(
        """
        SELECT
            g.nom_arrondissement AS arrondissement,
            s.annee,
            SUM(s.nombre) AS incidents_total
        FROM fact_securite s
        JOIN dim_geographie_arrondissement g ON g.code_arrondissement = s.code_arrondissement
        GROUP BY g.nom_arrondissement, s.annee
        """
    )
    merged = demography.merge(
        security,
        on=["arrondissement", "annee"],
        how="inner",
    )
    if merged.empty:
        return merged

    summary = (
        merged.sort_values(["arrondissement", "annee"])
        .groupby("arrondissement", as_index=False)
        .agg(
            annee_min=("annee", "min"),
            annee_max=("annee", "max"),
            population_debut=("population", "first"),
            population_fin=("population", "last"),
            insecurite_debut=("incidents_total", "first"),
            insecurite_fin=("incidents_total", "last"),
        )
    )
    summary["delta_population"] = summary["population_fin"] - summary["population_debut"]
    summary["delta_insecurite"] = summary["insecurite_fin"] - summary["insecurite_debut"]
    summary["cas_critique"] = (summary["delta_population"] <= 0) & (summary["delta_insecurite"] > 0)
    return summary


def build_demography_security_figure(summary: pd.DataFrame):
    if summary.empty:
        return px.scatter(title="Aucune donnee disponible pour demographie vs insecurite")

    fig = px.scatter(
        summary,
        x="delta_population",
        y="delta_insecurite",
        color="cas_critique",
        text="arrondissement",
        title="Evolution demographique vs evolution de l'insecurite (debut -> fin)",
        labels={
            "delta_population": "Variation population",
            "delta_insecurite": "Variation incidents",
            "cas_critique": "Pas de croissance demo + insecurite en hausse",
        },
        color_discrete_map={True: "#d62728", False: "#1f77b4"},
    )
    fig.add_hline(y=0, line_dash="dash")
    fig.add_vline(x=0, line_dash="dash")
    fig.update_traces(textposition="top center")
    return fig


def build_dashboard() -> Dash:
    ensure_mysql_data_loaded()

    nvp_by_arrdt, metric_label, metric_year = load_nvp_by_arrondissement()
    participation = load_participation_by_bureau()

    if not nvp_by_arrdt.empty and not participation.empty:
        nvp_latest = nvp_by_arrdt[nvp_by_arrdt["annee"] == int(nvp_by_arrdt["annee"].max())].copy()
        nvp_latest["arrdt_norm"] = nvp_latest["arrondissement"].map(normalize_arrondissement)
        participation["arrdt_norm"] = participation["arrondissement"].map(normalize_arrondissement)
        bureau_with_income = participation.merge(
            nvp_latest[["arrdt_norm", "niveau_vie_moyen"]],
            on="arrdt_norm",
            how="left",
        )
        bureau_with_income = bureau_with_income.dropna(
            subset=["niveau_vie_moyen", "taux_participation"]
        )
    else:
        bureau_with_income = pd.DataFrame(
            columns=[
                "id_bureau",
                "arrondissement",
                "tour",
                "inscrits",
                "taux_participation",
                "niveau_vie_moyen",
            ]
        )

    scatter_fig = build_scatter_figure(bureau_with_income, metric_label, metric_year)
    top_bureaux_fig = build_top_bureaux_figure(bureau_with_income)

    demo_security_summary = load_demography_security_trends()
    demo_security_fig = build_demography_security_figure(demo_security_summary)

    if "cas_critique" in demo_security_summary.columns:
        critical_arrondissements = demo_security_summary[
            demo_security_summary["cas_critique"]
        ].copy()
    else:
        critical_arrondissements = pd.DataFrame(
            columns=[
                "arrondissement",
                "annee_min",
                "annee_max",
                "delta_population",
                "delta_insecurite",
            ]
        )

    if not critical_arrondissements.empty:
        critical_arrondissements = critical_arrondissements.sort_values(
            "delta_insecurite", ascending=False
        )

    app = dash.Dash(__name__)
    app.title = "Lyon BI Dashboard"

    app.layout = html.Div(
        [
            html.H1("Dashboard BI - Elections, niveau de vie et insecurite"),
            html.P(
                "Focus: arrondissements sans croissance demographique et insecurite en hausse, "
                "plus croisement niveau de vie et participation par bureau."
            ),
            dcc.Graph(figure=demo_security_fig),
            html.H2("Arrondissements critiques"),
            dash_table.DataTable(
                columns=[
                    {"name": "Arrondissement", "id": "arrondissement"},
                    {"name": "Annee debut", "id": "annee_min"},
                    {"name": "Annee fin", "id": "annee_max"},
                    {"name": "Delta population", "id": "delta_population"},
                    {"name": "Delta insecurite", "id": "delta_insecurite"},
                ],
                data=critical_arrondissements[
                    [
                        "arrondissement",
                        "annee_min",
                        "annee_max",
                        "delta_population",
                        "delta_insecurite",
                    ]
                ].to_dict("records")
                if not critical_arrondissements.empty
                else [],
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "padding": "8px"},
                style_header={"fontWeight": "bold"},
                page_size=10,
            ),
            dcc.Graph(figure=scatter_fig),
            dcc.Graph(figure=top_bureaux_fig),
        ],
        style={"maxWidth": "1200px", "margin": "0 auto", "padding": "16px"},
    )
    return app


app = build_dashboard()


def _is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.3)
        return sock.connect_ex((host, port)) == 0


if __name__ == "__main__":
    port = int(os.getenv("DASH_PORT", "8050"))
    if _is_port_open("127.0.0.1", port):
        print(f"Dash app already running on http://localhost:{port}")
        raise SystemExit(0)

    app.run(host="0.0.0.0", port=port, debug=False)
