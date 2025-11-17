from typing import Dict, List, Any
import json

from fastapi import FastAPI, HTTPException
from pydantic import RootModel
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

app = FastAPI()


class TablesPayload(RootModel[Dict[str, List[Dict[str, Any]]]]):
    pass


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


producer = create_producer()


def validate_payload(data: Dict[str, List[Dict[str, Any]]]) -> None:
    if not data:
        raise HTTPException(status_code=400, detail="Передан пустой payload")

    for table_name, rows in data.items():
        if not isinstance(table_name, str) or not table_name:
            raise HTTPException(status_code=400, detail="Имя таблицы должно быть непустой строкой")

        if not isinstance(rows, list) or not rows:
            raise HTTPException(
                status_code=400,
                detail=f"Таблица '{table_name}' должна содержать непустой список строк",
            )

        first_row = rows[0]
        if not isinstance(first_row, dict):
            raise HTTPException(
                status_code=400,
                detail=f"В таблице '{table_name}' все строки должны быть словарями",
            )

        base_keys = set(first_row.keys())
        if not base_keys:
            raise HTTPException(
                status_code=400,
                detail=f"В таблице '{table_name}' строки должны содержать хотя бы одну колонку",
            )

        for idx, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                raise HTTPException(
                    status_code=400,
                    detail=f"В таблице '{table_name}' строка №{idx} не является словарем",
                )
            if set(row.keys()) != base_keys:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"В таблице '{table_name}' строка №{idx} имеет другой набор колонок.\n"
                        f"Ожидалось: {sorted(base_keys)}, получено: {sorted(row.keys())}"
                    ),
                )


@app.post("/data")
def send_data(payload: TablesPayload):
    tables: Dict[str, List[Dict[str, Any]]] = payload.root

    validate_payload(tables)

    message = {"tables": tables}
    try:
        future = producer.send(KAFKA_TOPIC, message)
        meta = future.get(timeout=10)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Не удалось отправить данные в Kafka: {e!r}")

    return {
        "status": "ok",
        "topic": meta.topic,
        "partition": meta.partition,
        "offset": meta.offset,
    }
