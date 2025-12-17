from typing import Optional
from schema_description import SCHEMA_DESCRIPTION
from local_llm_client import call_llm


SYSTEM_PROMPT_SQL = """
Ты — ассистент по аналитике данных.
Твоя задача: переводить запросы пользователя на естественном языке в SQL-запросы для SQLite.

Правила:
- Используй ТОЛЬКО таблицы и поля из описанной схемы.
- Пиши ТОЛЬКО сам SQL-запрос, без пояснений, без Markdown, без комментариев.
- Синтаксис должен быть совместим с SQLite.
- Если нужны фильтры по году, используй либо strftime('%Y', date_column) = '2024',
  либо сравнение с диапазоном дат.
"""

SYSTEM_PROMPT_EXPLANATION = """
Ты — аналитик по продажам.
Пишешь краткие и понятные интерпретации данных для менеджеров.
Формат: 1–3 предложения, без лишней воды, по-русски.
"""


def build_sql_prompt(user_query: str) -> str:
    return f"""
Схема базы данных:

{SCHEMA_DESCRIPTION}

Преобразуй следующий запрос пользователя в один корректный SQL-запрос (SQLite).

Запрос пользователя:
\"\"\"{user_query}\"\"\"
"""


def natural_language_explanation_prompt(user_query: str, sql: str, df_preview: str) -> str:
    return f"""
Пользователь задал аналитический вопрос:
\"\"\"{user_query}\"\"\"

Ты сгенерировал и выполнил SQL-запрос:
\"\"\"{sql}\"\"\"

Первые строки результата:
{df_preview}

Сделай короткое (1–3 предложения) объяснение результата на понятном русском языке
для менеджера по продажам. Укажи ключевые выводы: кто топ, какие категории,
какие города или сегменты выделяются и т.п.
"""


def _cleanup_sql(sql: str) -> str:
    """
    Удаляет возможные обертки ```sql ... ``` и лишний мусор.
    """
    if not sql:
        return ""

    s = sql.strip()
    if s.startswith("```"):
        # убираем все бэктики
        s = s.strip("`").strip()
        # убираем возможное "sql" в начале
        if s.lower().startswith("sql"):
            s = s[3:].lstrip("\n\r ")

    # на всякий случай обрезаем ; в конце
    return s.strip().rstrip(";")


def generate_sql_from_nl(user_query: str) -> str:
    """
    Генерирует SQL-запрос из текста пользователя через локальную LLM.
    """
    prompt = build_sql_prompt(user_query)
    raw_sql = call_llm(prompt, system_prompt=SYSTEM_PROMPT_SQL)
    sql = _cleanup_sql(raw_sql)
    return sql


def generate_text_explanation(user_query: str, sql: str, df) -> str:
    """
    Генерирует короткое текстовое объяснение результата запроса.
    """
    df_preview = df.head().to_string()
    prompt = natural_language_explanation_prompt(user_query, sql, df_preview)
    explanation = call_llm(prompt, system_prompt=SYSTEM_PROMPT_EXPLANATION)
    return explanation.strip()
