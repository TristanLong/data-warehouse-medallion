"""Singleton SQL Server connection for the DataWarehouse (pymssql, pure pip, no ODBC).

Usage:
    from src.database.db_connection import DatabaseConnection

    db = DatabaseConnection()          # always the same instance
    conn = db.get_connection()         # reuses one live pymssql connection
    rows = db.execute("SELECT TOP 5 * FROM gold.dim_customers")
    rows = db.execute("SELECT * FROM silver.crm_cust_info WHERE cst_id = %s", (11000,))
    db.close()

Settings come from environment variables (see .env.example), no defaults:
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
Query parameters use the pymssql style (%s), not the pyodbc style (?).
"""

import threading

import pymssql

from src.config.settings import get_env


class DatabaseConnection:
    """Thread-safe Singleton that owns a single pymssql connection."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        # Double-checked locking: cheap check first, lock only on first creation.
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._connection = None
                    cls._instance = instance
        return cls._instance

    @staticmethod
    def _connect() -> pymssql.Connection:
        return pymssql.connect(
            server=get_env("DB_HOST"),
            port=get_env("DB_PORT"),
            database=get_env("DB_NAME"),
            user=get_env("DB_USER"),
            password=get_env("DB_PASSWORD"),
        )

    def get_connection(self) -> pymssql.Connection:
        """Return the shared connection, (re)connecting if needed."""
        with self._lock:
            if self._connection is None or not self._is_alive():
                self._connection = self._connect()
            return self._connection

    def _is_alive(self) -> bool:
        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except pymssql.Error:
            return False

    def execute(self, query: str, params: tuple = ()) -> list:
        """Run a query and return all rows (empty list for statements without a result set)."""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or None)
            rows = cursor.fetchall() if cursor.description else []
            conn.commit()
            return rows
        except pymssql.Error:
            conn.rollback()
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        """Close the shared connection. The next get_connection() reconnects."""
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None
