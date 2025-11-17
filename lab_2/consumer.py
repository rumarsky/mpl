import json
from typing import Dict, List, Any, Optional, Iterable

from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql

from config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
)


def get_db_connection():
    return psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
    )


def infer_pg_type(values: Iterable[Any]) -> str:
    non_null = [v for v in values if v is not None]
    if not non_null:
        return "TEXT"

    if all(isinstance(v, bool) for v in non_null):
        return "BOOLEAN"

    if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
        return "BIGINT"

    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null):
        return "DOUBLE PRECISION"

    return "TEXT"


def create_table_if_not_exists(
    conn,
    table_name: str,
    rows: List[Dict[str, Any]],
):
    if not rows:
        return

    columns = list(rows[0].keys())

    col_types: Dict[str, str] = {}
    for col in columns:
        col_values = [row.get(col) for row in rows]
        col_types[col] = infer_pg_type(col_values)

    col_defs = [
        sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_types[col]))
        for col in columns
    ]

    query = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({fields})").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(", ").join(col_defs),
    )

    with conn.cursor() as cur:
        print(f"Создаю таблицу (если не существует): {table_name}")
        cur.execute(query)
    conn.commit()


def insert_rows(
    conn,
    table_name: str,
    rows: List[Dict[str, Any]],
):
    if not rows:
        return

    columns = list(rows[0].keys())

    col_idents = [sql.Identifier(col) for col in columns]
    placeholders = [sql.Placeholder() for _ in columns]

    query = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES ({values})"
    ).format(
        table=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(col_idents),
        values=sql.SQL(", ").join(placeholders),
    )

    values_list = [tuple(row[col] for col in columns) for row in rows]

    with conn.cursor() as cur:
        print(f"Вставляю {len(rows)} строк в таблицу {table_name}")
        cur.executemany(query, values_list)
    conn.commit()


def process_message(conn, message_value: Dict[str, Any]):
    tables = message_value.get("tables")
    if not isinstance(tables, dict):
        print("Неверный формат сообщения: поле 'tables' отсутствует или не является словарем")
        return

    for table_name, rows in tables.items():
        if not isinstance(table_name, str):
            print(f"Некорректное имя таблицы: {table_name!r}")
            continue

        if not isinstance(rows, list) or not rows:
            print(f"Таблица '{table_name}' не содержит корректных строк")
            continue

        try:
            create_table_if_not_exists(conn, table_name, rows)
            insert_rows(conn, table_name, rows)
            print(f"Таблица '{table_name}' успешно обработана")
        except Exception as e:
            conn.rollback()
            print(f"Не удалось обработать таблицу '{table_name}': {e!r}")


def main():
    print(f"Подключение к Kafka: {KAFKA_BOOTSTRAP_SERVERS}, топик '{KAFKA_TOPIC}'")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="tables-consumer-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    conn = get_db_connection()
    print("Подключение к PostgreSQL успешно установлено")

    try:
        for msg in consumer:
            print(f"Получено сообщение, offset={msg.offset}")
            try:
                process_message(conn, msg.value)
            except Exception as e:
                print(f"Непредвиденная ошибка при обработке сообщения: {e!r}")
    finally:
        conn.close()
        print("Соединение с PostgreSQL закрыто")


if __name__ == "__main__":
    main()
