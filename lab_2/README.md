1. python3 consumer.py
2. uvicorn app:app --reload
3. docker exec -it pg psql -U postgres -d postgres

{
  "products": [
    { "id": 1, "name": "Ноутбук", "price": 79999.99, "in_stock": true },
    { "id": 2, "name": "Мышь", "price": 999.5, "in_stock": false },
    { "id": 3, "name": "Клавиатура", "price": 2499, "in_stock": true }
  ],
  "customers": [
    { "id": 1, "email": "ivan@example.com", "age": 28 },
    { "id": 2, "email": "maria@example.com", "age": 31 },
    { "id": 3, "email": "test@example.com", "age": null }
  ],
  "payments": [
    { "id": 1, "customer_id": 1, "amount": 500.0, "success": true },
    { "id": 2, "customer_id": 2, "amount": 1000.5, "success": true },
    { "id": 3, "customer_id": 3, "amount": 0, "success": false }
  ]
}
