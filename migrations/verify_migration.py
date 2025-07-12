import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
from dotenv import load_dotenv
import os

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

def verify_locations():
    print("\nVerifying locations...")
    # Connect to PostgreSQL
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    
    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    
    # Get counts
    pg_cur.execute("SELECT COUNT(*) as count FROM locations")
    pg_count = pg_cur.fetchone()["count"]
    
    mongo_count = mongo_db.locations.count_documents({})
    
    print(f"PostgreSQL locations: {pg_count}")
    print(f"MongoDB locations: {mongo_count}")
    
    if pg_count != mongo_count:
        print("❌ Count mismatch in locations!")
    else:
        print("✓ Location counts match")
        
    # Check sample data
    if pg_count > 0:
        pg_cur.execute("SELECT * FROM locations LIMIT 1")
        pg_sample = pg_cur.fetchone()
        mongo_sample = mongo_db.locations.find_one({"pg_id": pg_sample["location_id"]})
        
        if mongo_sample:
            print("\nSample location check:")
            print(f"PostgreSQL: {pg_sample['name']}, {pg_sample['state']}")
            print(f"MongoDB:    {mongo_sample['name']}, {mongo_sample['state']}")
            if pg_sample["name"] == mongo_sample["name"] and pg_sample["state"] == mongo_sample["state"]:
                print("✓ Sample data matches")
            else:
                print("❌ Sample data mismatch!")
    
    pg_cur.close()
    pg_conn.close()

def verify_observations():
    print("\nVerifying observations...")
    # Connect to PostgreSQL
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    
    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    
    # Get counts
    pg_cur.execute("SELECT COUNT(*) as count FROM weather_observations")
    pg_count = pg_cur.fetchone()["count"]
    
    mongo_count = mongo_db.weather_observations.count_documents({})
    
    print(f"PostgreSQL observations: {pg_count}")
    print(f"MongoDB observations: {mongo_count}")
    
    if pg_count != mongo_count:
        print("❌ Count mismatch in observations!")
    else:
        print("✓ Observation counts match")
        
    # Check sample data
    if pg_count > 0:
        pg_cur.execute("SELECT * FROM weather_observations LIMIT 1")
        pg_sample = pg_cur.fetchone()
        mongo_sample = mongo_db.weather_observations.find_one({"pg_id": pg_sample["observation_id"]})
        
        if mongo_sample:
            print("\nSample observation check:")
            print(f"PostgreSQL: Location ID: {pg_sample['location_id']}, Date: {pg_sample['date']}")
            print(f"MongoDB:    Location ID: {mongo_sample['location_id']}, Date: {mongo_sample['date']}")
            if str(pg_sample['location_id']) == str(mongo_sample['location_id']):
                print("✓ Sample location_id matches")
            else:
                print("❌ Sample location_id mismatch!")
    
    pg_cur.close()
    pg_conn.close()

def verify_predictions():
    print("\nVerifying predictions...")
    # Connect to PostgreSQL
    pg_conn = get_pg_connection()
    pg_cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    
    # Connect to MongoDB
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    
    # Get counts
    pg_cur.execute("SELECT COUNT(*) as count FROM rain_predictions")
    pg_count = pg_cur.fetchone()["count"]
    
    mongo_count = mongo_db.rain_predictions.count_documents({})
    
    print(f"PostgreSQL predictions: {pg_count}")
    print(f"MongoDB predictions: {mongo_count}")
    
    if pg_count != mongo_count:
        print("❌ Count mismatch in predictions!")
    else:
        print("✓ Prediction counts match")
        
    # Check sample data
    if pg_count > 0:
        pg_cur.execute("SELECT * FROM rain_predictions LIMIT 1")
        pg_sample = pg_cur.fetchone()
        mongo_sample = mongo_db.rain_predictions.find_one({"pg_id": pg_sample["prediction_id"]})
        
        if mongo_sample:
            print("\nSample prediction check:")
            print(f"PostgreSQL: Observation ID: {pg_sample['observation_id']}, Will Rain: {pg_sample['will_it_rain']}")
            print(f"MongoDB:    Observation ID: {mongo_sample['observation_id']}, Will Rain: {mongo_sample['will_it_rain']}")
            if (str(pg_sample['observation_id']) == str(mongo_sample['observation_id']) and 
                pg_sample['will_it_rain'] == mongo_sample['will_it_rain']):
                print("✓ Sample data matches")
            else:
                print("❌ Sample data mismatch!")
    
    pg_cur.close()
    pg_conn.close()

def main():
    print("Verifying data migration from PostgreSQL to MongoDB...")
    
    try:
        verify_locations()
        verify_observations()
        verify_predictions()
        print("\nVerification completed!")
        
    except Exception as e:
        print(f"Error during verification: {str(e)}")

if __name__ == "__main__":
    main()
