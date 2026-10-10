from src.database.db_connection import DatabaseConnection
from src.etl.run_pipeline import run_pipeline
from src.utils.spark_cache import clear_spark_cache

if __name__ == "__main__":
    db = DatabaseConnection()
    try:
        db.get_connection()
        print("Connected to the database.")
        run_pipeline()
    finally:
        db.close()
        print("Connection closed.")
        #clear_spark_cache()  # comment this line to keep the downloaded JDBC jar between runs
