#!/bin/bash
# Tester la connectivité MySQL depuis l'environnement de l'application
#
# Ce script tente de se connecter au serveur MySQL en utilisant les variables
# d'environnement fournies. Il est utile pour le débogage des problèmes de réseau
# ou d'authentification dans les configurations Docker.

set -e

echo "Vérification de la connexion MySQL..."

# Utilisation des valeurs par défaut si les variables ne sont pas définies
HOST=${MYSQL_HOST:-mysql}
PORT=${MYSQL_PORT:-3306}
USER=${MYSQL_USER:-root}
PASSWORD=${MYSQL_PASSWORD:-}
DATABASE=${MYSQL_DATABASE:-lyon_decisional}

python3 - << EOF
import mysql.connector
import sys
import os

try:
    conn = mysql.connector.connect(
        host="$HOST",
        port=$PORT,
        user="$USER",
        password="$PASSWORD",
        database="$DATABASE"
    )
    print(f"✅ Succès : Connecté à {conn.get_server_info()} sur $HOST")
    conn.close()
    sys.exit(0)
except Exception as e:
    print(f"❌ Erreur : Impossible de se connecter à MySQL sur $HOST")
    print(f"Détail : {e}")
    sys.exit(1)
EOF
