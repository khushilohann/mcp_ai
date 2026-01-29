import os

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.getenv("SQLITE_DB_PATH", os.path.join(BASE_DIR, "sample.db"))
