import os

from dotenv import load_dotenv


load_dotenv()

DSL = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASS"),
    "host": os.environ.get("DB_HOST", "127.0.0.1"),
    "port": os.environ.get("DB_PORT", 5432),
    "options": "-c search_path=content",
}

ES_HOST = os.environ.get("ES_HOST", "http://127.0.0.1:9200")
FILE_PATH = os.environ.get("FILE_PATH", "data.json")
INTERVAL = os.environ.get("INTERVAL", 300)
