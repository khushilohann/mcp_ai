import asyncio
import os
import aiosqlite
from datetime import datetime, timedelta
from .session import DB_PATH

async def seed():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  region TEXT,
  signup_date TEXT
);
CREATE TABLE IF NOT EXISTS products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  price REAL
);
CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  product_id INTEGER,
  quantity INTEGER,
  order_date TEXT,
  FOREIGN KEY(user_id) REFERENCES users(id),
  FOREIGN KEY(product_id) REFERENCES products(id)
);
""")
        # Clear existing sample rows for idempotence
        await db.execute("DELETE FROM users")
        await db.execute("DELETE FROM products")
        await db.execute("DELETE FROM orders")

        # Insert products
        products = [("Widget", 9.99), ("Gadget", 19.99), ("Doodad", 4.99)]
        for p in products:
            await db.execute("INSERT INTO products (name, price) VALUES (?, ?)", p)

        # Generate 200 users
        regions = ["NA", "EU", "APAC", "LATAM"]
        base_date = datetime(2025, 1, 1)
        
        users = []
        for i in range(1, 201):
            name = f"User{i}"
            email = f"user{i}@example.com"
            region = regions[i % len(regions)]
            signup_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            users.append((name, email, region, signup_date))
        
        for u in users:
            await db.execute("INSERT INTO users (name, email, region, signup_date) VALUES (?,?,?,?)", u)

        # Generate orders for users (150 orders total)
        for i in range(1, 151):
            user_id = (i % 200) + 1
            product_id = (i % 3) + 1
            quantity = (i % 5) + 1
            order_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
            await db.execute(
                "INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES (?, ?, ?, ?)",
                (user_id, product_id, quantity, order_date)
            )

        await db.commit()
    print(f"Seeded DB at {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(seed())
