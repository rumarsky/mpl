import re

ALLOWED_STATEMENTS = ("select",)


def is_safe_sql(query: str) -> bool:
    """
    Простая верификация SQL:
    - Разрешаем только SELECT
    - Запрещаем потенциально опасные команды
    - Ограничиваемся таблицами orders, products, customers
    """
    if not query:
        return False

    q = query.strip().lower()

    # только select в начале
    if not q.startswith(ALLOWED_STATEMENTS):
        return False

    # запрет на опасные ключевые слова
    forbidden = ["insert", "update", "delete", "drop", "alter", "truncate", "create", "attach", "replace"]
    if any(word in q for word in forbidden):
        return False

    # whitelisting таблиц
    allowed_tables = {"orders", "products", "customers"}

    # ищем from / join
    table_pattern = r"(from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)"
    for match in re.finditer(table_pattern, q):
        table = match.group(2)
        if table not in allowed_tables:
            return False

    return True
