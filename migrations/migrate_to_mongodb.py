import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
from datetime import datetime, date

# Load environment variables
load_dotenv()

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

def get_pg_connection():
    return psycopg2.connect(**PG_CONN)

def initialize_mongodb():
    print("Initializing MongoDB database...")
    try:
        # Connect to MongoDB with ServerApi
        mongo_client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
        
        # Verify connection with ping
        mongo_client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        
        # Get database and create initialization document
        mongo_db = mongo_client[MONGO_DB]
        mongo_db.init.insert_one({"initialized": True, "timestamp": datetime.utcnow()})
        print("MongoDB database initialized successfully!")
        
        return mongo_client
    except Exception as e:
        print(f"Error initializing MongoDB database: {str(e)}")
        raise

def migrate_locations():
    print("Migrating locations...")
    # Connect to PostgreSQL
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    
    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    
    # Clear existing locations in MongoDB
    mongo_db.locations.delete_many({})
    
    # Get all locations from PostgreSQL
    pg_cur.execute("SELECT * FROM locations")
    locations = pg_cur.fetchall()
    
    # Insert into MongoDB
    if locations:
        location_docs = []
        for loc in locations:
            location_docs.append({
                "name": loc["name"],
                "state": loc["state"],
                "pg_id": loc["location_id"]  # Keep track of PostgreSQL ID
            })
        
        mongo_db.locations.insert_many(location_docs)
    
    print(f"Migrated {len(locations)} locations")
    pg_cur.close()
    pg_conn.close()

def migrate_observations():
    print("Migrating observations...")
    # Connect to PostgreSQL
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    
    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    
    # Clear existing observations in MongoDB
    mongo_db.weather_observations.delete_many({})
    
    # Get all observations from PostgreSQL
    pg_cur.execute("SELECT * FROM weather_observations")
    observations = pg_cur.fetchall()
    
    # Insert into MongoDB
    if observations:
        observation_docs = []
        for obs in observations:
            # Convert to dict and handle datetime/date objects
            obs_dict = dict(obs)
            obs_dict["pg_id"] = obs_dict["observation_id"]  # Keep track of PostgreSQL ID
            del obs_dict["observation_id"]
            
            # Convert date to datetime
            if isinstance(obs_dict["date"], date):
                obs_dict["date"] = datetime.combine(obs_dict["date"], datetime.min.time())
            
            observation_docs.append(obs_dict)
        
        mongo_db.weather_observations.insert_many(observation_docs)
    
    print(f"Migrated {len(observations)} observations")
    pg_cur.close()
    pg_conn.close()

def migrate_predictions():
    print("Migrating predictions...")
    # Connect to PostgreSQL
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    
    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    
    # Clear existing predictions in MongoDB
    mongo_db.rain_predictions.delete_many({})
    
    # Get all predictions from PostgreSQL
    pg_cur.execute("SELECT * FROM rain_predictions")
    predictions = pg_cur.fetchall()
    
    # Insert into MongoDB
    if predictions:
        prediction_docs = []
        for pred in predictions:
            # Convert to dict and handle datetime objects
            pred_dict = dict(pred)
            pred_dict["pg_id"] = pred_dict["prediction_id"]  # Keep track of PostgreSQL ID
            del pred_dict["prediction_id"]
            
            prediction_docs.append(pred_dict)
        
        mongo_db.rain_predictions.insert_many(prediction_docs)
    
    print(f"Migrated {len(predictions)} predictions")
    pg_cur.close()
    pg_conn.close()

def migrate_all(mongo_client):
    mongo_db = mongo_client[MONGO_DB]
    
    try:
        # Migrate in order of dependencies
        migrate_locations()
        migrate_observations()
        migrate_predictions()
        print("Migration completed successfully!")
    except Exception as e:
        print(f"Error during migration: {str(e)}")
    finally:
        mongo_client.close()

def main():
    print("Starting data migration from PostgreSQL to MongoDB...")
    
    try:
        # Initialize MongoDB and get client
        mongo_client = initialize_mongodb()
        if mongo_client:
            migrate_all(mongo_client)
        
    except Exception as e:
        print(f"Error during migration: {str(e)}")

if __name__ == "__main__":
    main()
