import os

# Set credentials for testing BEFORE importing dash_app because of top-level calls
os.environ["MYSQL_USER"] = "lyon_user"
os.environ["MYSQL_PASSWORD"] = "lyon_secure_password_2026"

from src.bi.dash_app import (  # noqa: E402
    load_global_metrics,
    load_votes_by_block,
)


def test():
    try:
        print("Testing load_votes_by_block...")
        df_votes = load_votes_by_block("ALL")
        print(f"df_votes: {len(df_votes)} rows")
        print(df_votes.head())

        print("\nTesting load_global_metrics...")
        df_global = load_global_metrics(2021)
        print(f"df_global: {len(df_global)} rows")
        print(df_global.head())

        if len(df_global) == 0:
            print("\nWARNING: df_global is empty!")
            # Investigate why df_global is empty
            # Step 1: Participation & Inscrits
            from src.bi.dash_app import read_sql

            df1 = read_sql("""
                SELECT
                    b.arrondissement,
                    AVG(p.taux_participation) as participation,
                    SUM(p.inscrits) as inscrits
                FROM fact_participation p
                JOIN dim_geographie_bureau b ON b.id_bureau = p.id_bureau
                GROUP BY b.arrondissement
            """)
            print(f"Step 1 (Participation): {len(df1)} rows")

            # Step 2: Sécurité + Population
            df2 = read_sql("""
                SELECT
                    g.nom_arrondissement as arrondissement,
                    SUM(s.nombre) as total_incidents,
                    MAX(d.population) as population
                FROM fact_securite s
                JOIN dim_geographie_arrondissement g
                    ON g.code_arrondissement = s.code_arrondissement
                JOIN fact_demographie_annuelle d
                    ON d.code_arrondissement = s.code_arrondissement
                WHERE s.annee = 2022 AND d.annee = 2022
                GROUP BY g.nom_arrondissement
            """)
            print(f"Step 2 (Security): {len(df2)} rows")

            # Step 3: Niveau de Vie
            df3 = read_sql(
                """
                SELECT
                    ga.nom_arrondissement as arrondissement,
                    SUM(f.somme_niveaux_de_vie_winsorises_des_individus) /
                    SUM(f.nb_individus) as revenu_moyen
                FROM fact_niveau_vie_pauvrete_200m f
                JOIN dim_geographie_200m g
                    ON g.sk_geographie = f.sk_geographie
                JOIN dim_geographie_arrondissement ga
                    ON ga.code_arrondissement = g.arrondissement
                WHERE f.sk_temps = %s
                GROUP BY ga.nom_arrondissement
            """,
                [2021],
            )
            print(f"Step 3 (Niveau de Vie): {len(df3)} rows")

    except Exception as e:
        print(f"Error during testing: {e}")


if __name__ == "__main__":
    test()
