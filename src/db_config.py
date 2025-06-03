from dotenv import load_dotenv
import os

load_dotenv()

db_config = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}
db_name = os.getenv("MYSQL_DB", "gehealthcare")
