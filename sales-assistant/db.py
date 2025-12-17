import sqlite3
import pandas as pd
from typing import Optional

DB_PATH = "sales.db"


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def run_query(sql: str) -> pd.DataFrame:
    """
    Выполняет SQL и возвращает результат как pandas DataFrame.
    """
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = get_connection()
        df = pd.read_sql_query(sql, conn)
    finally:
        if conn is not None:
            conn.close()
    return df
