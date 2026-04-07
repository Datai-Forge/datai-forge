"""
Database module for Lyon Decisional System.

This module re-exports the database loader for backward compatibility.
For initialization, use src.database_loader instead.

Usage:
    python3 -m src.database_loader
"""

from src.database_loader import (
    DB_CONFIG,
    connect_with_retry,
    create_database_if_not_exists,
    create_tables,
    main,
)

__all__ = [
    "DB_CONFIG",
    "connect_with_retry",
    "create_database_if_not_exists",
    "create_tables",
    "main",
]

# This module is safe to import; no automatic execution.
# To initialize the database, run: python3 -m src.database_loader
