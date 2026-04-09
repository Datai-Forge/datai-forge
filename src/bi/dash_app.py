"""Dash BI application for normalized security and election analysis in Lyon."""

import os
import re

import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

# --- DATABASE HELPERS ---


def get_connection():
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


# --- DATA LOADING ---


def load_votes_by_block(arrondissement: str = "ALL") -> pd.DataFrame:
    where, params = ("", [])
    if arrondissement != "ALL":
        where, params = ("WHERE b.arrondissement = %s", [arrondissement])
    sql = f"""
        SELECT c.bloc_analytique, SUM(v.voix) as total_voix
        FROM fact_votes v
        JOIN dim_candidats c ON c.id_candidat = v.id_candidat
        JOIN dim_geographie_bureau b ON b.id_bureau = v.id_bureau
        {where}
        GROUP BY c.bloc_analytique
    """
    return read_sql(sql, params)


def load_global_metrics():
    """Aggregates Participation and NORMALIZED Security data."""
    # 1. Participation & Inscrits
    df = read_sql("""
        SELECT
            b.arrondissement,
            AVG(p.taux_participation) as participation,
            SUM(p.inscrits) as inscrits
        FROM fact_participation p
        JOIN dim_geographie_bureau b ON b.id_bureau = p.id_bureau
        GROUP BY b.arrondissement
    """)

    # 2. Sécurité + Population (pour normalisation)
    # On prend la population la plus récente (2022 ou max)
    df_sec_pop = read_sql("""
        SELECT
            g.nom_arrondissement as arrondissement,
            SUM(s.nombre) as total_incidents,
            MAX(d.population) as population
        FROM fact_securite s
        JOIN dim_geographie_arrondissement g ON g.code_arrondissement = s.code_arrondissement
        JOIN fact_demographie_annuelle d ON d.code_arrondissement = s.code_arrondissement
        WHERE s.annee = 2022 AND d.annee = 2022
        GROUP BY g.nom_arrondissement
    """)

    def clean_name(name):
        if not name:
            return ""
        match = re.search(r"(\d+)", str(name))
        if match:
            if match.group(1) == "1":
                return f"{match.group(1)}er"
            return f"{match.group(1)}ème"
        return name

    df["arrdt_key"] = df["arrondissement"].apply(clean_name)
    df_sec_pop["arrdt_key"] = df_sec_pop["arrondissement"].apply(clean_name)

    merged = df.merge(
        df_sec_pop[["arrdt_key", "total_incidents", "population"]], on="arrdt_key", how="left"
    )

    # Calcul du taux pour 1000 habitants
    merged["taux_incidents"] = (merged["total_incidents"] / merged["population"]) * 1000
    return merged.fillna(0)


def get_arrondissements():
    sql = (
        "SELECT DISTINCT arrondissement "
        "FROM dim_geographie_bureau "
        "WHERE arrondissement IS NOT NULL"
    )
    return read_sql(sql)["arrondissement"].tolist()


# --- APP SETUP ---

app = Dash(__name__)
app.title = "Lyon BI - Sécurité Normalisée"

POLITICAL_COLORS = {
    "EXTREME_GAUCHE": "#7D0000",
    "GAUCHE": "#E60000",
    "CENTRE": "#FFC400",
    "DROITE": "#0066CC",
    "EXTREME_DROITE": "#003366",
}
CARD = {
    "backgroundColor": "white",
    "padding": "20px",
    "borderRadius": "12px",
    "boxShadow": "0 4px 15px rgba(0,0,0,0.05)",
    "marginBottom": "20px",
}

app.layout = html.Div(
    style={"backgroundColor": "#f0f2f5", "padding": "30px", "fontFamily": "Segoe UI, sans-serif"},
    children=[
        html.H1(
            "Analyse Électorale et Sécuritaire Normalisée",
            style={"textAlign": "center", "color": "#1a2a6c", "fontWeight": "bold"},
        ),
        html.Div(
            [
                html.Label("📍 Focus par Arrondissement :", style={"fontWeight": "bold"}),
                dcc.Dropdown(
                    id="arrdt-selector",
                    options=[{"label": "Vue d'ensemble (Lyon)", "value": "ALL"}]
                    + [{"label": a, "value": a} for a in get_arrondissements()],
                    value="ALL",
                    clearable=False,
                ),
            ],
            style={"width": "450px", "margin": "30px auto"},
        ),
        html.Div(id="kpi-row", style={"display": "flex", "gap": "20px", "marginBottom": "30px"}),
        html.Div(
            style={"display": "flex", "gap": "20px"},
            children=[
                html.Div(
                    style={"flex": "1"},
                    children=[
                        html.Div(
                            [
                                html.H3("Rapport de Force Politique", style={"marginTop": "0"}),
                                dcc.Graph(id="pie-chart"),
                            ],
                            style=CARD,
                        ),
                    ],
                ),
                html.Div(
                    style={"flex": "1.5"},
                    children=[
                        html.Div(
                            [
                                html.H3("Corrélation Participation vs Taux de Délinquance"),
                                html.P(
                                    "La courbe montre le nombre d'incidents pour 1000 habitants "
                                    "(Indicateur neutre de la démographie).",
                                    style={
                                        "color": "#7f8c8d",
                                        "fontSize": "13px",
                                        "marginBottom": "20px",
                                    },
                                ),
                                dcc.Graph(id="ranking-chart"),
                            ],
                            style={**CARD, "height": "100%"},
                        )
                    ],
                ),
            ],
        ),
    ],
)

# --- CALLBACKS ---


@app.callback(
    [
        Output("pie-chart", "figure"),
        Output("ranking-chart", "figure"),
        Output("kpi-row", "children"),
    ],
    [Input("arrdt-selector", "value")],
)
def update_dashboard(selected_arrdt):
    df_votes = load_votes_by_block(selected_arrdt)
    df_global = load_global_metrics()

    # 1. Pie Chart
    pie_fig = px.pie(
        df_votes,
        values="total_voix",
        names="bloc_analytique",
        color="bloc_analytique",
        color_discrete_map=POLITICAL_COLORS,
        hole=0.5,
    )
    pie_fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))

    # 2. Ranking Chart (Normalized)
    df_rank = df_global.sort_values("participation", ascending=False)
    rank_fig = go.Figure()

    rank_fig.add_trace(
        go.Bar(
            x=df_rank["arrondissement"],
            y=df_rank["participation"],
            name="Participation (%)",
            marker_color="#3498db",
            opacity=0.7,
        )
    )

    rank_fig.add_trace(
        go.Scatter(
            x=df_rank["arrondissement"],
            y=df_rank["taux_incidents"],
            name="Incidents / 1000 hab.",
            yaxis="y2",
            line=dict(color="#e74c3c", width=3),
            mode="lines+markers",
        )
    )

    rank_fig.update_layout(
        title="Zoom : Détail des Écarts de Participation",
        yaxis=dict(
            title="Taux de Participation (%)", range=[70, 85]
        ),  # Zoom pour voir les 10% d'écart
        yaxis2=dict(title="Incidents pour 1000 hab.", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=40, b=20),
    )

    if selected_arrdt != "ALL":
        opacity_list = [1.0 if a == selected_arrdt else 0.2 for a in df_rank["arrondissement"]]
        rank_fig.update_traces(
            marker_opacity=opacity_list,
            selector=dict(type="bar"),
        )

    # 3. KPIs
    if selected_arrdt == "ALL":
        part = df_global["participation"].mean()
        tot_ins = df_global["inscrits"].sum()
        pop = df_global["population"].sum()
    else:
        row = df_global[df_global["arrondissement"] == selected_arrdt].iloc[0]
        part, tot_ins, pop = row["participation"], row["inscrits"], row["population"]

    kpis = [
        html.Div(
            [html.H2(f"{part:.1f}%"), html.P("Participation")],
            style={**CARD, "flex": "1", "textAlign": "center", "borderLeft": "5px solid #3498db"},
        ),
        html.Div(
            [html.H2(f"{tot_ins:,}".replace(",", " ")), html.P("Inscrits")],
            style={**CARD, "flex": "1", "textAlign": "center", "borderLeft": "5px solid #2ecc71"},
        ),
        html.Div(
            [html.H2(f"{pop:,.0f}".replace(",", " ")), html.P("Population (Hab.)")],
            style={**CARD, "flex": "1", "textAlign": "center", "borderLeft": "5px solid #9b59b6"},
        ),
    ]

    return pie_fig, rank_fig, kpis


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
