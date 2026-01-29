import asyncio
from app.services.api_connector import APIConnector

async def main():
    base = "http://127.0.0.1:9001"
    connector = APIConnector(base, api_key="demo-key")

    print("Creating item...")
    item = await connector.post("/items", json={"name": "Demo Item", "description": "Created by demo script"})
    print("Created:", item)

    print("Listing items...")
    items = await connector.get("/items")
    print("Items:", items)

    item_id = item.get("id")
    print("Updating item...", item_id)
    updated = await connector.put(f"/items/{item_id}", json={"name": "Demo Item v2", "description": "Updated"})
    print("Updated:", updated)

    print("Deleting item...", item_id)
    res = await connector.delete(f"/items/{item_id}")
    print("Delete result:", res)

    await connector.close()

if __name__ == "__main__":
    asyncio.run(main())
