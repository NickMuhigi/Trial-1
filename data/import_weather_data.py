import pandas as pd
import psycopg2
from pymongo import MongoClient
from datetime import datetime

# --- CONFIG ---
CSV_PATH = 'weatherAUS.csv'
# PostgreSQL (Supabase) connection
PG_CONN = {
    'host': 'aws-0-eu-central-1.pooler.supabase.com',
    'port': 6543,
    'dbname': 'postgres',
    'user': 'postgres.fdmizgsixffqbpbjqdey',
    'password': 'xZ5i6iLA7yZLsJNX'
}
# MongoDB connection
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'weather_db'

# --- LOAD CSV ---
df = pd.read_csv(CSV_PATH, nrows=200)

# --- POSTGRESQL IMPORT ---
def import_to_postgres(df):
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    # Insert unique locations
    locations = df[['Location']].drop_duplicates().reset_index(drop=True)
    location_id_map = {}
    for i, row in locations.iterrows():
        cur.execute("INSERT INTO locations (name) VALUES (%s) RETURNING location_id", (row['Location'],))
        location_id = cur.fetchone()[0]
        location_id_map[row['Location']] = location_id
        if (i + 1) % 100 == 0:
            print(f"Inserted {i+1} locations into PostgreSQL...")
    conn.commit()
    # Insert weather observations in chunks for efficiency
    chunk_size = 10
    weather_data = []
    for idx, row in df.iterrows():
        loc_id = location_id_map.get(row['Location'])
        weather_data.append((
            loc_id,
            row['Date'],
            row.get('MinTemp'),
            row.get('MaxTemp'),
            row.get('Rainfall'),
            row.get('Humidity9am'),
            row.get('Humidity3pm'),
            row.get('Pressure9am'),
            row.get('Pressure3pm'),
            row.get('WindSpeed9am'),
            row.get('WindSpeed3pm'),
            row.get('WindDir9am'),
            row.get('WindDir3pm'),
            row.get('Cloud9am'),
            row.get('Cloud3pm'),
            row.get('Temp9am'),
            row.get('Temp3pm'),
            True if row.get('RainToday') == 'Yes' else False,
            True if row.get('RainTomorrow') == 'Yes' else False
        ))
        # Insert in batches
        if len(weather_data) >= chunk_size:
            cur.executemany("""
                INSERT INTO weather_observations (location_id, date, min_temp, max_temp, rainfall, 
                humidity_9am, humidity_3pm, pressure_9am, pressure_3pm, wind_speed_9am, wind_speed_3pm, 
                wind_dir_9am, wind_dir_3pm, cloud_9am, cloud_3pm, temp_9am, temp_3pm, rain_today, rain_tomorrow)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, weather_data)
            conn.commit()
            print(f"Inserted {idx+1} rows into PostgreSQL...")
            weather_data = []
    # Insert any remaining data
    if weather_data:
        cur.executemany("""
            INSERT INTO weather_observations (location_id, date, min_temp, max_temp, rainfall, 
            humidity_9am, humidity_3pm, pressure_9am, pressure_3pm, wind_speed_9am, wind_speed_3pm, 
            wind_dir_9am, wind_dir_3pm, cloud_9am, cloud_3pm, temp_9am, temp_3pm, rain_today, rain_tomorrow)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, weather_data)
        conn.commit()
    cur.close()
    conn.close()

# --- MONGODB IMPORT ---
def import_to_mongo(df):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    # Insert locations
    locations = df[['Location']].drop_duplicates().reset_index(drop=True)
    location_id_map = {}
    for i, row in locations.iterrows():
        loc_doc = {'name': row['Location']}
        result = db.locations.insert_one(loc_doc)
        location_id_map[row['Location']] = result.inserted_id
        if (i + 1) % 100 == 0:
            print(f"Inserted {i+1} locations into MongoDB...")
    # Insert weather observations in batches
    chunk_size = 10
    obs_docs = []
    for idx, row in df.iterrows():
        loc_id = location_id_map.get(row['Location'])
        obs_doc = {
            'location_id': loc_id,
            'date': datetime.strptime(row['Date'], '%Y-%m-%d'),
            'min_temp': row.get('MinTemp'),
            'max_temp': row.get('MaxTemp'),
            'rainfall': row.get('Rainfall'),
            'humidity_9am': row.get('Humidity9am'),
            'humidity_3pm': row.get('Humidity3pm'),
            'pressure_9am': row.get('Pressure9am'),
            'pressure_3pm': row.get('Pressure3pm'),
            'wind_speed_9am': row.get('WindSpeed9am'),
            'wind_speed_3pm': row.get('WindSpeed3pm'),
            'wind_dir_9am': row.get('WindDir9am'),
            'wind_dir_3pm': row.get('WindDir3pm'),
            'cloud_9am': row.get('Cloud9am'),
            'cloud_3pm': row.get('Cloud3pm'),
            'temp_9am': row.get('Temp9am'),
            'temp_3pm': row.get('Temp3pm'),
            'rain_today': row.get('RainToday') == 'Yes',
            'rain_tomorrow': row.get('RainTomorrow') == 'Yes'
        }
        obs_docs.append(obs_doc)
        if len(obs_docs) >= chunk_size:
            db.weather_observations.insert_many(obs_docs)
            print(f"Inserted {idx+1} rows into MongoDB...")
            obs_docs = []
    if obs_docs:
        db.weather_observations.insert_many(obs_docs)
    client.close()

if __name__ == '__main__':
    # Skip PostgreSQL since it's already done
    import_to_postgres(df)
    import_to_mongo(df)
    print('Data import complete!')
