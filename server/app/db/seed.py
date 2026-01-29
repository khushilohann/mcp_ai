import asyncio
import os
import aiosqlite
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

        users = [
            ("Alice", "alice@example.com", "NA", "2025-12-01"),
            ("Bob", "bob@example.com", "EU", "2025-12-15"),
            ("Carol", "carol@example.com", "APAC", "2026-01-10"),
        ]
        for u in users:
            await db.execute("INSERT INTO users (name, email, region, signup_date) VALUES (?,?,?,?)", u)

        products = [("Widget", 9.99), ("Gadget", 19.99), ("Doodad", 4.99)]
        for p in products:
            await db.execute("INSERT INTO products (name, price) VALUES (?, ?)", p)

        await db.execute("INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES (1, 1, 2, '2026-01-05')")
        await db.execute("INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES (2, 2, 1, '2026-01-15')")
        await db.execute("INSERT INTO orders (user_id, product_id, quantity, order_date) VALUES (1, 3, 5, '2026-01-20')")

        await db.commit()
    print(f"Seeded DB at {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(seed())
