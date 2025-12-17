import sqlite3
import random
from datetime import datetime, timedelta

DB_PATH = "sales.db"


def create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        category TEXT,
        price REAL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        customer_name TEXT,
        city TEXT,
        segment TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        order_date DATE,
        product_id INTEGER,
        customer_id INTEGER,
        quantity INTEGER,
        total_amount REAL,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );
    """)

    conn.commit()


def seed_products(conn: sqlite3.Connection, n: int = 30) -> None:
    cur = conn.cursor()

    categories = ["Electronics", "Clothing", "Groceries", "Toys", "Home", "Sports"]
    products = []
    for i in range(1, n + 1):
        category = random.choice(categories)
        product_name = f"{category} Item {i}"
        price = round(random.uniform(5.0, 500.0), 2)
        products.append((i, product_name, category, price))

    cur.executemany("""
        INSERT OR REPLACE INTO products (product_id, product_name, category, price)
        VALUES (?, ?, ?, ?);
    """, products)

    conn.commit()


def seed_customers(conn: sqlite3.Connection, n: int = 50) -> None:
    cur = conn.cursor()

    cities = ["Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg", "Kazan"]
    segments = ["retail", "b2b"]
    customers = []
    for i in range(1, n + 1):
        customer_name = f"Customer {i}"
        city = random.choice(cities)
        segment = random.choice(segments)
        customers.append((i, customer_name, city, segment))

    cur.executemany("""
        INSERT OR REPLACE INTO customers (customer_id, customer_name, city, segment)
        VALUES (?, ?, ?, ?);
    """, customers)

    conn.commit()


def random_date(start_year=2023, end_year=2024) -> str:
    """
    Возвращает случайную дату в строковом формате YYYY-MM-DD
    между 01-01-start_year и 31-12-end_year.
    """
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    delta_days = (end_date - start_date).days
    random_days = random.randint(0, delta_days)
    d = start_date + timedelta(days=random_days)
    return d.strftime("%Y-%m-%d")


def seed_orders(conn: sqlite3.Connection, n: int = 1000) -> None:
    cur = conn.cursor()

    # Получаем диапазон product_id и customer_id
    cur.execute("SELECT MIN(product_id), MAX(product_id) FROM products;")
    min_pid, max_pid = cur.fetchone()

    cur.execute("SELECT MIN(customer_id), MAX(customer_id) FROM customers;")
    min_cid, max_cid = cur.fetchone()

    if min_pid is None or min_cid is None:
        raise RuntimeError("Сначала нужно создать продукты и клиентов.")

    orders = []
    for i in range(1, n + 1):
        order_date = random_date(2023, 2024)
        product_id = random.randint(min_pid, max_pid)
        customer_id = random.randint(min_cid, max_cid)
        quantity = random.randint(1, 10)

        # Получим цену товара
        cur.execute("SELECT price FROM products WHERE product_id = ?;", (product_id,))
        price_row = cur.fetchone()
        price = price_row[0] if price_row else random.uniform(10.0, 200.0)

        total_amount = round(price * quantity, 2)
        orders.append((i, order_date, product_id, customer_id, quantity, total_amount))

    cur.executemany("""
        INSERT OR REPLACE INTO orders (order_id, order_date, product_id, customer_id, quantity, total_amount)
        VALUES (?, ?, ?, ?, ?, ?);
    """, orders)

    conn.commit()


def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        create_tables(conn)
        seed_products(conn, n=40)
        seed_customers(conn, n=60)
        seed_orders(conn, n=1200)
        print("База данных sales.db успешно создана и заполнена тестовыми данными.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
