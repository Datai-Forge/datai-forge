"""Dash BI application for normalized security and election analysis in Lyon."""

import os
import re
import warnings

import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html

# Suppress pandas UserWarning for non-SQLAlchemy connections
warnings.filterwarnings(
    "ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*"
)
pd.set_option("future.no_silent_downcasting", True)

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


def load_global_metrics(poverty_year: int = 2021):
    """Aggregates Participation, NORMALIZED Security, and Standard of Living data."""
    # 1. Participation & Inscrits (Election 2022)
    df = read_sql("""
        SELECT
            b.arrondissement,
            AVG(p.taux_participation) as participation,
            SUM(p.inscrits) as inscrits
        FROM fact_participation p
        JOIN dim_geographie_bureau b ON b.id_bureau = p.id_bureau
        GROUP BY b.arrondissement
    """)

    # 2. Sécurité + Population (pour normalisation) - Match closest to election or latest available
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

    # 3. Niveau de Vie (Revenu Moyen) & Age Groups - Dynamic year
    df_rev_age = read_sql(
        """
        SELECT
            ga.nom_arrondissement as arrondissement,
            SUM(f.somme_niveaux_de_vie_winsorises_des_individus) /
            SUM(f.nb_individus) as revenu_moyen,
            SUM(f.nb_individus) as population_social,
            SUM(f.nb_menages) as nb_menages,
            SUM(f.nb_individus_18_24_ans) as age_18_24,
            SUM(f.nb_individus_25_39_ans) as age_25_39,
            SUM(f.nb_individus_40_54_ans) as age_40_54,
            SUM(f.nb_individus_55_64_ans) as age_55_64,
            SUM(f.nb_individus_65_79_ans) as age_65_79,
            SUM(f.nb_individus_80p_ans) as age_80p
        FROM fact_niveau_vie_pauvrete_200m f
        JOIN dim_geographie_200m g ON g.sk_geographie = f.sk_geographie
        JOIN dim_geographie_arrondissement ga ON ga.code_arrondissement = g.arrondissement
        WHERE f.sk_temps = %s
        GROUP BY ga.nom_arrondissement
    """,
        [poverty_year],
    )

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
    df_rev_age["arrdt_key"] = df_rev_age["arrondissement"].apply(clean_name)

    merged = df.merge(
        df_sec_pop[["arrdt_key", "total_incidents", "population"]], on="arrdt_key", how="left"
    )
    merged = merged.merge(
        df_rev_age[
            [
                "arrdt_key",
                "revenu_moyen",
                "nb_menages",
                "population_social",
                "age_18_24",
                "age_25_39",
                "age_40_54",
                "age_55_64",
                "age_65_79",
                "age_80p",
            ]
        ],
        on="arrdt_key",
        how="left",
    )

    # Calcul du taux pour 1000 habitants
    merged["taux_incidents"] = (merged["total_incidents"] / merged["population"]) * 1000
    # Calcul de la taille des ménages
    merged["taille_menage"] = merged["population_social"] / merged["nb_menages"]
    return merged.fillna(0)


def load_household_size_history() -> pd.DataFrame:
    """Loads household size history for all available years (2017, 2019, 2021)."""
    sql = """
        SELECT
            ga.nom_arrondissement as arrondissement,
            t.annee,
            SUM(f.nb_individus) / SUM(f.nb_menages) as taille_menage
        FROM fact_niveau_vie_pauvrete_200m f
        JOIN dim_geographie_200m g ON g.sk_geographie = f.sk_geographie
        JOIN dim_geographie_arrondissement ga ON ga.code_arrondissement = g.arrondissement
        JOIN dim_temps t ON t.sk_temps = f.sk_temps
        GROUP BY ga.nom_arrondissement, t.annee
        ORDER BY t.annee
    """
    df = read_sql(sql)

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
    return df


def normalize_arrondissement_name(name: str) -> str:
    if not name:
        return ""
    match = re.search(r"(\d+)", str(name))
    if match:
        if match.group(1) == "1":
            return f"{match.group(1)}er"
        return f"{match.group(1)}ème"
    return str(name)


def get_arrondissements():
    sql = (
        "SELECT DISTINCT arrondissement "
        "FROM dim_geographie_bureau "
        "WHERE arrondissement IS NOT NULL"
    )
    return read_sql(sql)["arrondissement"].tolist()


# --- APP SETUP ---

app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "Lyon BI - Sécurité et Social"

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
            "Analyse Électorale, Sécuritaire et Sociale",
            style={"textAlign": "center", "color": "#1a2a6c", "fontWeight": "bold"},
        ),
        html.Div(
            style={
                "display": "flex",
                "justifyContent": "center",
                "gap": "20px",
                "margin": "30px auto",
            },
            children=[
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
                    style={"width": "300px"},
                ),
                html.Div(
                    [
                        html.Label("📅 Année (Niveau de Vie):", style={"fontWeight": "bold"}),
                        dcc.Dropdown(
                            id="year-selector",
                            options=[
                                {"label": "2017", "value": 2017},
                                {"label": "2019", "value": 2019},
                                {"label": "2021", "value": 2021},
                            ],
                            value=2021,
                            clearable=False,
                        ),
                    ],
                    style={"width": "200px"},
                ),
            ],
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
                                    "La courbe montre le nombre d'incidents pour 1000 habitants.",
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
        html.Div(
            style={"display": "flex", "gap": "20px", "marginTop": "100px"},
            children=[
                html.Div(
                    [
                        html.H3("Relation Participation et Niveau de Vie"),
                        html.P(
                            id="poverty-chart-title",
                            children=(
                                "Analyse de la participation en fonction du "
                                "revenu moyen par habitant."
                            ),
                            style={"color": "#7f8c8d", "fontSize": "13px"},
                        ),
                        dcc.Graph(id="poverty-chart"),
                    ],
                    style={**CARD, "flex": "1"},
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(
                                    "📍 Filtre arrondissement (ménages) :",
                                    style={"fontWeight": "bold"},
                                ),
                                dcc.Dropdown(
                                    id="household-arrdt-selector",
                                    options=[{"label": "Tous les arrondissements", "value": "ALL"}]
                                    + [{"label": a, "value": a} for a in get_arrondissements()],
                                    value="ALL",
                                    clearable=False,
                                ),
                            ],
                            style={"marginBottom": "15px"},
                        ),
                        html.H3("Évolution de la Taille des Ménages"),
                        html.P(
                            "Taille moyenne des ménages (Nombre d'individus par ménage).",
                            style={"color": "#7f8c8d", "fontSize": "13px"},
                        ),
                        dcc.Graph(id="household-size-chart"),
                    ],
                    style={**CARD, "flex": "1"},
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
        Output("poverty-chart", "figure"),
        Output("household-size-chart", "figure"),
        Output("kpi-row", "children"),
        Output("poverty-chart-title", "children"),
    ],
    [
        Input("arrdt-selector", "value"),
        Input("year-selector", "value"),
        Input("household-arrdt-selector", "value"),
    ],
)
def update_dashboard(selected_arrdt, selected_year, household_arrdt):
    df_votes = load_votes_by_block(selected_arrdt)
    df_global = load_global_metrics(selected_year)
    df_household_hist = load_household_size_history()

    def get_sort_key(name):
        match = re.search(r"(\d+)", str(name))
        return int(match.group(1)) if match else 99

    # Prepare sorted data for Arrondissements 1 to 9
    df_sorted = df_global.copy()
    df_sorted["sort_idx"] = df_sorted["arrondissement"].apply(get_sort_key)
    df_sorted = df_sorted.sort_values("sort_idx")
    # Filter to only keep 1st to 9th
    df_sorted = df_sorted[df_sorted["sort_idx"] <= 9]

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
        yaxis=dict(title="Taux de Participation (%)", range=[70, 85]),
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

    # 3. Poverty/Standard of Living Chart
    pov_fig = px.scatter(
        df_global,
        x="revenu_moyen",
        y="participation",
        text="arrondissement",
        size="inscrits",
        color="revenu_moyen",
        color_continuous_scale="Viridis",
        labels={
            "revenu_moyen": f"Revenu Moyen (€) - {selected_year}",
            "participation": "Participation (%)",
            "arrondissement": "Arrondissement",
        },
        trendline="ols",
    )
    pov_fig.update_traces(textposition="top center")
    pov_fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=40, b=20),
    )

    if selected_arrdt != "ALL":
        colors = [
            "#e74c3c" if a == selected_arrdt else "#bdc3c7" for a in df_global["arrondissement"]
        ]
        pov_fig.update_traces(marker=dict(color=colors, size=15))

    # 4. Household Size Chart
    hh_fig = go.Figure()
    if household_arrdt == "ALL":
        for arrdt_name in sorted(df_household_hist["arrondissement"].dropna().unique()):
            df_arr = df_household_hist[
                df_household_hist["arrondissement"] == arrdt_name
            ].sort_values("annee")
            hh_fig.add_trace(
                go.Scatter(
                    x=df_arr["annee"],
                    y=df_arr["taille_menage"],
                    mode="lines+markers",
                    name=arrdt_name,
                )
            )
        title_hh = "Évolution de la Taille des Ménages - Tous les arrondissements"
    else:
        household_arrdt_key = normalize_arrondissement_name(household_arrdt)
        df_hh_plot = df_household_hist[
            df_household_hist["arrdt_key"] == household_arrdt_key
        ].sort_values("annee")
        hh_fig.add_trace(
            go.Scatter(
                x=df_hh_plot["annee"],
                y=df_hh_plot["taille_menage"],
                mode="lines+markers",
                name=household_arrdt,
                line=dict(color="#9b59b6", width=3),
            )
        )
        title_hh = f"Évolution de la Taille des Ménages - {household_arrdt}"

    hh_fig.update_layout(
        title=title_hh,
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(dtick=1, tickmode="linear", tickformat="d"),
        yaxis=dict(title="Taille Moyenne"),
    )

    # 6. KPIs
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

    title_text = (
        f"Analyse de la participation (Election 2022) en fonction du "
        f"revenu moyen par habitant (Données Insee {selected_year})."
    )

    return pie_fig, rank_fig, pov_fig, hh_fig, kpis, title_text


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
