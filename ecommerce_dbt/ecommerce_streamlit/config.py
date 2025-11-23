import os
import snowflake.connector
from dotenv import load_dotenv

# carrega .env se existir
load_dotenv()


def get_snowflake_connection():
    """Abre conexão com o Snowflake usando variáveis de ambiente."""
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    return conn


def run_query(sql: str):
    """Executa uma query e devolve um DataFrame pandas."""
    import pandas as pd

    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        df = cur.fetch_pandas_all()
    finally:
        cur.close()
        conn.close()
    return df
