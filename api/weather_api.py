from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from datetime import datetime, date
from bson import ObjectId
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo.errors import PyMongoError
from fastapi.exceptions import RequestValidationError
from typing import List

from api.database import get_db_connection, db_mongo
from api.models import (
    Location, LocationBase,
    Observation, ObservationBase,
    Prediction, PredictionBase,
    MongoLocation, MongoLocationBase,
    MongoObservation, MongoObservationBase,
    MongoPrediction, MongoPredictionBase
)

app = FastAPI(title="Rainfall Prediction API")

def verify_location_exists(location_id: int) -> bool:
    """
    Verify that a location exists in MongoDB before creating related records.
    Returns True if location exists, raises HTTPException if not.
    """
    location = db_mongo.locations.find_one({"location_id": location_id})
    if not location:
        raise HTTPException(
            status_code=404,
            detail=f"Location with ID {location_id} not found. Please create the location first."
        )
    return True

# Setup MongoDB Schema Validation
def setup_mongodb_validation():
    try:
        # Weather Observations Schema
        db_mongo.command('collMod', 'weather_observations', 
            validator={'$jsonSchema': {
                'bsonType': 'object',
                'required': ['observation_id', 'location_id', 'date'],
                'properties': {
                    'observation_id': {'bsonType': 'int'},
                    'location_id': {'bsonType': 'int'},
                    'date': {'bsonType': 'date'},
                    'min_temp': {'bsonType': ['double', 'null']},
                    'max_temp': {'bsonType': ['double', 'null']},
                    'rainfall': {'bsonType': ['double', 'null']},
                    'humidity_9am': {'bsonType': ['double', 'null']},
                    'humidity_3pm': {'bsonType': ['double', 'null']},
                    'pressure_9am': {'bsonType': ['double', 'null']},
                    'pressure_3pm': {'bsonType': ['double', 'null']},
                    'wind_speed_9am': {'bsonType': ['double', 'null']},
                    'wind_speed_3pm': {'bsonType': ['double', 'null']},
                    'wind_dir_9am': {'bsonType': ['string', 'null']},
                    'wind_dir_3pm': {'bsonType': ['string', 'null']},
                    'cloud_9am': {'bsonType': ['double', 'null']},
                    'cloud_3pm': {'bsonType': ['double', 'null']},
                    'temp_9am': {'bsonType': ['double', 'null']},
                    'temp_3pm': {'bsonType': ['double', 'null']},
                    'rain_today': {'bsonType': ['bool', 'null']},
                    'rain_tomorrow': {'bsonType': ['bool', 'null']}
                }
            }},
            validationLevel='strict'
        )
        
        # Locations Schema
        db_mongo.command('collMod', 'locations', 
            validator={'$jsonSchema': {
                'bsonType': 'object',
                'required': ['location_id', 'name'],
                'properties': {
                    'location_id': {'bsonType': 'int'},
                    'name': {'bsonType': 'string'},
                    'state': {'bsonType': ['string', 'null']}
                }
            }},
            validationLevel='strict'
        )
        
        # Rain Predictions Schema
        db_mongo.command('collMod', 'rain_predictions', 
            validator={'$jsonSchema': {
                'bsonType': 'object',
                'required': ['prediction_id', 'location_id', 'observation_id', 'will_it_rain', 'predicted_at'],
                'properties': {
                    'prediction_id': {'bsonType': 'int'},
                    'location_id': {'bsonType': 'int'},
                    'observation_id': {'bsonType': 'int'},
                    'will_it_rain': {'bsonType': 'bool'},
                    'predicted_at': {'bsonType': 'date'}
                }
            }},
            validationLevel='strict'
        )
    except Exception as e:
        print(f"Warning: Could not set up MongoDB validation: {str(e)}")

# Initialize MongoDB schema validation on startup
@app.on_event("startup")
async def startup_event():
    setup_mongodb_validation()

# Helper functions for MongoDB sequential IDs
def get_next_sequence_value(collection_name: str) -> int:
    """Get the next sequential ID for a collection"""
    sequence_document = db_mongo.counters.find_one_and_update(
        {"_id": collection_name},
        {"$inc": {"sequence_value": 1}},
        upsert=True,
        return_document=True
    )
    return sequence_document["sequence_value"]

# Exception handlers for robust error handling
@app.exception_handler(psycopg2.Error)
async def postgres_exception_handler(request: Request, exc: psycopg2.Error):
    return JSONResponse(status_code=500, content={"detail": "PostgreSQL error", "error": str(exc)})

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"detail": "Internal server error", "error": str(exc)})

# Handle HTTPException separately to preserve status codes
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

# MongoDB error handler
@app.exception_handler(PyMongoError)
async def mongo_exception_handler(request: Request, exc: PyMongoError):
    return JSONResponse(status_code=500, content={"detail": "MongoDB error", "error": str(exc)})

# Validation errors handler
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={"detail": exc.errors(), "body": exc.body})

# CRUD endpoints for Locations
@app.post("/locations/", response_model=Location)
def create_location(loc: LocationBase):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO locations (name, state) VALUES (%s, %s) RETURNING location_id, name, state",
        (loc.name, loc.state)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return Location(location_id=row[0], name=row[1], state=row[2])

@app.get("/locations/", response_model=List[Location])
def read_locations():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT location_id, name, state FROM locations")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@app.get("/locations/{location_id}", response_model=Location)
def read_location(location_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT location_id, name, state FROM locations WHERE location_id = %s",
        (location_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Location not found")
    return Location(location_id=row[0], name=row[1], state=row[2])

@app.put("/locations/{location_id}", response_model=Location)
def update_location(location_id: int, loc: LocationBase):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE locations SET name = %s, state = %s WHERE location_id = %s RETURNING location_id, name, state",
        (loc.name, loc.state, location_id)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Location not found")
    return Location(location_id=row[0], name=row[1], state=row[2])

@app.delete("/locations/{location_id}", status_code=204)
def delete_location(location_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM locations WHERE location_id = %s", (location_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Location not found")
    conn.commit()
    cur.close()
    conn.close()

# CRUD endpoints for Observations
@app.post("/observations/", response_model=Observation)
def create_observation(obs: ObservationBase):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO weather_observations (
            location_id, date, min_temp, max_temp, rainfall,
            humidity_9am, humidity_3pm, pressure_9am, pressure_3pm,
            wind_speed_9am, wind_speed_3pm, wind_dir_9am,
            wind_dir_3pm, cloud_9am, cloud_3pm, temp_9am, temp_3pm,
            rain_today, rain_tomorrow
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING observation_id
    """, (
        obs.location_id, obs.date, obs.min_temp, obs.max_temp,
        obs.rainfall, obs.humidity_9am, obs.humidity_3pm,
        obs.pressure_9am, obs.pressure_3pm,
        obs.wind_speed_9am, obs.wind_speed_3pm,
        obs.wind_dir_9am, obs.wind_dir_3pm,
        obs.cloud_9am, obs.cloud_3pm, obs.temp_9am,
        obs.temp_3pm, obs.rain_today, obs.rain_tomorrow
    ))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return Observation(observation_id=row[0], **obs.dict())

@app.get("/observations/", response_model=List[Observation])
def read_observations():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM weather_observations
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@app.get("/observations/{observation_id}", response_model=Observation)
def read_observation(observation_id: int):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM weather_observations WHERE observation_id = %s", (observation_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Observation not found")
    return row

@app.put("/observations/{observation_id}", response_model=Observation)
def update_observation(observation_id: int, obs: ObservationBase):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        UPDATE weather_observations SET
            location_id=%s, date=%s, min_temp=%s, max_temp=%s, rainfall=%s,
            humidity_9am=%s, humidity_3pm=%s, pressure_9am=%s, pressure_3pm=%s,
            wind_speed_9am=%s, wind_speed_3pm=%s, wind_dir_9am=%s,
            wind_dir_3pm=%s, cloud_9am=%s, cloud_3pm=%s, temp_9am=%s,
            temp_3pm=%s, rain_today=%s, rain_tomorrow=%s
        WHERE observation_id=%s RETURNING *
    """, (
        obs.location_id, obs.date, obs.min_temp, obs.max_temp,
        obs.rainfall, obs.humidity_9am, obs.humidity_3pm,
        obs.pressure_9am, obs.pressure_3pm, obs.wind_speed_9am,
        obs.wind_speed_3pm, obs.wind_dir_9am, obs.wind_dir_3pm,
        obs.cloud_9am, obs.cloud_3pm, obs.temp_9am, obs.temp_3pm,
        obs.rain_today, obs.rain_tomorrow, observation_id
    ))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Observation not found")
    return Observation(**dict(row))

@app.delete("/observations/{observation_id}", status_code=204)
def delete_observation(observation_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM weather_observations WHERE observation_id = %s", (observation_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Observation not found")
    conn.commit()
    cur.close()
    conn.close()

# CRUD endpoints for Predictions
@app.post("/predictions/", response_model=Prediction)
def create_prediction(pred: PredictionBase):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO rain_predictions (observation_id, will_it_rain) VALUES (%s, %s) RETURNING prediction_id, predicted_at",
        (pred.observation_id, pred.will_it_rain)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return Prediction(prediction_id=row[0], observation_id=pred.observation_id, will_it_rain=pred.will_it_rain, predicted_at=row[1])

@app.get("/predictions/", response_model=List[Prediction])
def read_predictions():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM rain_predictions")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

@app.get("/predictions/{prediction_id}", response_model=Prediction)
def read_prediction(prediction_id: int):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM rain_predictions WHERE prediction_id = %s", (prediction_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Prediction not found")
    return row

@app.put("/predictions/{prediction_id}", response_model=Prediction)
def update_prediction(prediction_id: int, pred: PredictionBase):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE rain_predictions SET observation_id=%s, will_it_rain=%s
        WHERE prediction_id=%s RETURNING prediction_id, observation_id, will_it_rain, predicted_at
    """, (pred.observation_id, pred.will_it_rain, prediction_id))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Prediction not found")
    return Prediction(prediction_id=row[0], observation_id=row[1], will_it_rain=row[2], predicted_at=row[3])

@app.delete("/predictions/{prediction_id}", status_code=204)
def delete_prediction(prediction_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM rain_predictions WHERE prediction_id = %s", (prediction_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Prediction not found")
    conn.commit()
    cur.close()
    conn.close()

# CRUD endpoints for MongoDB Locations
@app.post("/mongo/locations/", response_model=dict)
def create_mongo_location(loc: MongoLocationBase):
    try:
        # Get next sequential ID
        location_id = get_next_sequence_value("locations")
        data = loc.dict()
        data["location_id"] = location_id
        result = db_mongo.locations.insert_one(data)
        created_loc = MongoLocation(**data)
        return {
            "message": f"Location '{loc.name}' created successfully with ID {location_id}",
            "data": created_loc.dict()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create location: {str(e)}")

@app.get("/mongo/locations/", response_model=dict)
def read_mongo_locations():
    try:
        docs = list(db_mongo.locations.find())
        locations = [MongoLocation(location_id=d["location_id"], name=d["name"], state=d.get("state")) for d in docs]
        return {
            "message": f"Found {len(locations)} locations",
            "data": [loc.dict() for loc in locations]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve locations: {str(e)}")

@app.get("/mongo/locations/{location_id}", response_model=dict)
def read_mongo_location(location_id: int):
    try:
        doc = db_mongo.locations.find_one({"location_id": location_id})
        if not doc:
            raise HTTPException(status_code=404, detail=f"Location with ID {location_id} not found")
        location = MongoLocation(location_id=doc["location_id"], name=doc["name"], state=doc.get("state"))
        return {
            "message": f"Location found: {location.name}",
            "data": location.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve location: {str(e)}")

@app.put("/mongo/locations/{location_id}", response_model=dict)
def update_mongo_location(location_id: int, loc: LocationBase):
    try:
        data = loc.dict()
        res = db_mongo.locations.update_one({"location_id": location_id}, {"$set": data})
        if res.matched_count == 0:
            raise HTTPException(status_code=404, detail=f"Location with ID {location_id} not found")
        doc = db_mongo.locations.find_one({"location_id": location_id})
        updated_loc = MongoLocation(location_id=doc["location_id"], name=doc["name"], state=doc.get("state"))
        return {
            "message": f"Location {location_id} updated successfully",
            "data": updated_loc.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update location: {str(e)}")

@app.delete("/mongo/locations/{location_id}", response_model=dict)
def delete_mongo_location(location_id: int):
    try:
        # First get the location name for the success message
        doc = db_mongo.locations.find_one({"location_id": location_id})
        if not doc:
            raise HTTPException(status_code=404, detail=f"Location with ID {location_id} not found")
        
        location_name = doc["name"]
        res = db_mongo.locations.delete_one({"location_id": location_id})
        if res.deleted_count == 0:
            raise HTTPException(status_code=404, detail=f"Location with ID {location_id} not found")
            
        return {
            "message": f"Location '{location_name}' (ID: {location_id}) deleted successfully",
            "data": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete location: {str(e)}")

# CRUD endpoints for MongoDB Observations
@app.post("/mongo/observations/", response_model=dict)
def create_mongo_observation(obs: ObservationBase):
    try:
        # Verify location exists
        verify_location_exists(obs.location_id)
        
        # Get next sequential ID
        observation_id = get_next_sequence_value("observations")
        
        data = obs.dict()
        # Convert date to datetime if it's a date object
        if isinstance(data["date"], date):
            data["date"] = datetime.combine(data["date"], datetime.min.time())
        
        data["observation_id"] = observation_id
        
        # Create the document in MongoDB
        result = db_mongo.weather_observations.insert_one(data)
        saved_data = db_mongo.weather_observations.find_one({"_id": result.inserted_id})
        created_obs = MongoObservation(**{k: saved_data.get(k) for k in saved_data if k != '_id'})
        
        return {
            "message": f"Weather observation created successfully with ID {observation_id}",
            "data": created_obs.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create observation: {str(e)}")

@app.get("/mongo/observations/", response_model=dict)
def read_mongo_observations():
    try:
        docs = list(db_mongo.weather_observations.find())
        observations = [MongoObservation(**{k: d.get(k) for k in d if k != '_id'}) for d in docs]
        return {
            "message": f"Found {len(observations)} weather observations",
            "data": [obs.dict() for obs in observations]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve observations: {str(e)}")

@app.get("/mongo/observations/{observation_id}", response_model=dict)
def read_mongo_observation(observation_id: int):
    try:
        doc = db_mongo.weather_observations.find_one({"observation_id": observation_id})
        if not doc:
            raise HTTPException(status_code=404, detail=f"Observation with ID {observation_id} not found")
        
        observation = MongoObservation(**{k: doc.get(k) for k in doc if k != '_id'})
        return {
            "message": f"Found observation for location {doc['location_id']} on {doc['date']}",
            "data": observation.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve observation: {str(e)}")

@app.put("/mongo/observations/{observation_id}", response_model=dict)
def update_mongo_observation(observation_id: int, obs: ObservationBase):
    try:
        # Verify location exists if location_id is being updated
        if obs.location_id:
            verify_location_exists(obs.location_id)
            
        data = obs.dict()
        # Convert date to datetime if it's a date object
        if isinstance(data["date"], date):
            data["date"] = datetime.combine(data["date"], datetime.min.time())
        
        res = db_mongo.weather_observations.update_one({"observation_id": observation_id}, {"$set": data})
        if res.matched_count == 0:
            raise HTTPException(status_code=404, detail=f"Observation with ID {observation_id} not found")
        
        doc = db_mongo.weather_observations.find_one({"observation_id": observation_id})
        updated_obs = MongoObservation(**{k: doc.get(k) for k in doc if k != '_id'})
        return {
            "message": f"Weather observation {observation_id} updated successfully",
            "data": updated_obs.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update observation: {str(e)}")

@app.delete("/mongo/observations/{observation_id}", response_model=dict)
def delete_mongo_observation(observation_id: int):
    try:
        # First get the observation details for the success message
        doc = db_mongo.weather_observations.find_one({"observation_id": observation_id})
        if not doc:
            raise HTTPException(status_code=404, detail=f"Observation with ID {observation_id} not found")
        
        location_id = doc["location_id"]
        observation_date = doc["date"]
        
        res = db_mongo.weather_observations.delete_one({"observation_id": observation_id})
        if res.deleted_count == 0:
            raise HTTPException(status_code=404, detail=f"Observation with ID {observation_id} not found")
            
        return {
            "message": f"Weather observation {observation_id} (Location: {location_id}, Date: {observation_date}) deleted successfully",
            "data": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete observation: {str(e)}")

# CRUD endpoints for MongoDB Predictions
@app.post("/mongo/predictions/", response_model=dict)
def create_mongo_prediction(pred: MongoPredictionBase):
    try:
        # Get next sequential ID
        prediction_id = get_next_sequence_value("predictions")
        
        # First get the observation
        observation = db_mongo.weather_observations.find_one({"observation_id": pred.observation_id})
        if not observation:
            raise HTTPException(
                status_code=404,
                detail=f"Observation with ID {pred.observation_id} not found"
            )
        
        # Verify location exists
        location_id = observation["location_id"]
        verify_location_exists(location_id)
        
        data = pred.dict()
        data["location_id"] = location_id
        data["prediction_id"] = prediction_id
        data["predicted_at"] = datetime.now()
        
        result = db_mongo.rain_predictions.insert_one(data)
        saved_data = db_mongo.rain_predictions.find_one({"_id": result.inserted_id})
        created_pred = MongoPrediction(**{k: saved_data.get(k) for k in saved_data if k != '_id'})
        
        rain_status = "rain expected" if pred.will_it_rain else "no rain expected"
        return {
            "message": f"Created prediction (ID: {prediction_id}) for location {location_id}: {rain_status}",
            "data": created_pred.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create prediction: {str(e)}")

@app.get("/mongo/predictions/", response_model=dict)
def read_mongo_predictions():
    try:
        docs = list(db_mongo.rain_predictions.find())
        predictions = [MongoPrediction(**{k: d.get(k) for k in d if k != '_id'}) for d in docs]
        return {
            "message": f"Found {len(predictions)} weather predictions",
            "data": [pred.dict() for pred in predictions]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve predictions: {str(e)}")

@app.get("/mongo/predictions/{prediction_id}", response_model=dict)
def read_mongo_prediction(prediction_id: int):
    try:
        doc = db_mongo.rain_predictions.find_one({"prediction_id": prediction_id})
        if not doc:
            raise HTTPException(status_code=404, detail=f"Prediction with ID {prediction_id} not found")
        
        prediction = MongoPrediction(**{k: doc.get(k) for k in doc if k != '_id'})
        rain_status = "rain expected" if doc["will_it_rain"] else "no rain expected"
        return {
            "message": f"Found prediction for location {doc['location_id']}: {rain_status} (made on {doc['predicted_at']})",
            "data": prediction.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve prediction: {str(e)}")

@app.put("/mongo/predictions/{prediction_id}", response_model=dict)
def update_mongo_prediction(prediction_id: int, pred: PredictionBase):
    try:
        # Get the existing prediction
        existing = db_mongo.rain_predictions.find_one({"prediction_id": prediction_id})
        if not existing:
            raise HTTPException(status_code=404, detail=f"Prediction with ID {prediction_id} not found")
        
        # If observation_id changed, verify the new observation exists and get its location_id
        if pred.observation_id != existing["observation_id"]:
            observation = db_mongo.weather_observations.find_one({"observation_id": pred.observation_id})
            if not observation:
                raise HTTPException(
                    status_code=404,
                    detail=f"Observation with ID {pred.observation_id} not found"
                )
            verify_location_exists(observation["location_id"])
            data["location_id"] = observation["location_id"]
        
        data = pred.dict()
        # Update predicted_at on every update
        data["predicted_at"] = datetime.now()
        
        res = db_mongo.rain_predictions.update_one({"prediction_id": prediction_id}, {"$set": data})
        if res.matched_count == 0:
            raise HTTPException(status_code=404, detail=f"Prediction with ID {prediction_id} not found")
        
        doc = db_mongo.rain_predictions.find_one({"prediction_id": prediction_id})
        updated_pred = MongoPrediction(**{k: doc.get(k) for k in doc if k != '_id'})
        rain_status = "rain expected" if pred.will_it_rain else "no rain expected"
        return {
            "message": f"Updated prediction {prediction_id}: {rain_status}",
            "data": updated_pred.dict()
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update prediction: {str(e)}")

@app.delete("/mongo/predictions/{prediction_id}", response_model=dict)
def delete_mongo_prediction(prediction_id: int):
    try:
        # First get the prediction details for the success message
        doc = db_mongo.rain_predictions.find_one({"prediction_id": prediction_id})
        if not doc:
            raise HTTPException(status_code=404, detail=f"Prediction with ID {prediction_id} not found")
        
        location_id = doc["location_id"]
        predicted_at = doc["predicted_at"]
        rain_status = "rain expected" if doc["will_it_rain"] else "no rain expected"
        
        res = db_mongo.rain_predictions.delete_one({"prediction_id": prediction_id})
        if res.deleted_count == 0:
            raise HTTPException(status_code=404, detail=f"Prediction with ID {prediction_id} not found")
            
        return {
            "message": f"Deleted prediction {prediction_id} (Location: {location_id}, {rain_status}, made on {predicted_at})",
            "data": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete prediction: {str(e)}")
