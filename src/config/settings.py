"""Central place for reading settings from environment variables (.env).

No defaults: a missing variable raises immediately so misconfiguration is obvious.
"""

import os

from dotenv import load_dotenv

load_dotenv()


def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable {name} (see .env.example)")
    return value


def jdbc_url() -> str:
    """JDBC URL for SQL Server, built from the DB_* variables."""
    return (
        f"jdbc:sqlserver://{get_env('DB_HOST')}:{get_env('DB_PORT')};"
        f"databaseName={get_env('DB_NAME')};encrypt=true;"
        f"trustServerCertificate={get_env('DB_TRUST_SERVER_CERTIFICATE')}"
    )


def jdbc_properties() -> dict:
    """Connection properties for spark.read.jdbc / df.write.jdbc."""
    return {
        "user": get_env("DB_USER"),
        "password": get_env("DB_PASSWORD"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
