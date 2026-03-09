#!/bin/bash
set -e  # Arrête le script si une commande échoue

echo "🚀 Démarrage de la Pipeline ETL"

echo "1/2 Ingestion Bronze..."
python3 -m src.etl.bronze.bronze_presidentielle

echo "2/2 Transformation Silver..."
python3 -m src.etl.silver.silver_presidentielle

echo "✅ Pipeline ETL terminée avec succès !"
