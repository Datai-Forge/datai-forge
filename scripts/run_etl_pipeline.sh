#!/bin/bash
set -e  # Arrête le script si une commande échoue

echo "🚀 Démarrage de la Pipeline ETL"

echo "1/3 Ingestion Bronze..."
python3 -m src.etl.bronze.bronze_niveau_vie_pauvrete_200m
python3 -m src.etl.bronze.bronze_presidentielle
python3 -m src.etl.bronze.bronze_securite

echo "2/3 Transformation Silver..."
python3 -m src.etl.silver.silver_presidentielle
python3 -m src.etl.silver.silver_niveau_vie_pauvrete_2017
python3 -m src.etl.silver.silver_niveau_vie_pauvrete_2019
python3 -m src.etl.silver.silver_securite

echo "3/3 Transformation Gold..."
python3 -m src.etl.gold.gold_presidentielle_bi
python3 -m src.etl.gold.gold_securite_bi


echo "✅ Pipeline ETL terminée avec succès !"
