SCHEMA_DESCRIPTION = """
У тебя есть база данных магазина с такими таблицами:

1) products
    - product_id (integer) — ID товара
    - product_name (text) — название товара
    - category (text) — категория товара
    - price (real) — цена за единицу

2) customers
    - customer_id (integer) — ID покупателя
    - customer_name (text) — имя покупателя
    - city (text) — город
    - segment (text) — тип клиента (например: 'retail', 'b2b')

3) orders
    - order_id (integer) — ID заказа
    - order_date (date) — дата заказа (формат YYYY-MM-DD)
    - product_id (integer) — ссылка на products.product_id
    - customer_id (integer) — ссылка на customers.customer_id
    - quantity (integer) — количество единиц товара
    - total_amount (real) — итоговая сумма заказа

Примеры типичных аналитических запросов:
- выручка по категориям
- топ товаров по выручке
- количество заказов по клиентам
- выручка по месяцам, городам, сегментам и т.п.
"""
