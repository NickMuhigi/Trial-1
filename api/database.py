import os
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()  # loads .env from current working directory

# PostgreSQL connection settings
PG_CONN = {
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT'),
    'dbname': os.getenv('PG_DBNAME'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
}

# MongoDB connection settings
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')

def get_db_connection():
    """Get a PostgreSQL database connection"""
    return psycopg2.connect(**PG_CONN)

# MongoDB client
client_mongo = MongoClient(MONGO_URI)
db_mongo = client_mongo[MONGO_DB]
