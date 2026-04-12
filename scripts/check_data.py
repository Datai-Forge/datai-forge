import os

import mysql.connector

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "mysql"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "lyon_user"),
    "password": os.getenv("MYSQL_PASSWORD", "lyon_secure_password_2026"),
    "database": os.getenv("MYSQL_DATABASE", "lyon_decisional"),
}


def check_tables():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"Tables found: {tables}")

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"Table {table}: {count} rows")

    # Check some data
    cursor.execute("SELECT * FROM fact_votes LIMIT 1")
    print(f"fact_votes sample: {cursor.fetchone()}")

    conn.close()


if __name__ == "__main__":
    check_tables()
