// Weather Database Configuration

// Database creation and user setup (run as admin)
db = db.getSiblingDB('weather_db');

// Collection schemas and validation
db.createCollection("locations", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["name"],
            properties: {
                name: {
                    bsonType: "string",
                    description: "Location name - required string"
                },
                state: {
                    bsonType: "string",
                    description: "State/territory code"
                }
            }
        }
    }
});

db.createCollection("weather_observations", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["location_id", "date"],
            properties: {
                location_id: {
                    bsonType: "objectId",
                    description: "Reference to locations collection"
                },
                date: {
                    bsonType: "date",
                    description: "Date of observation"
                },
                min_temp: { bsonType: "double" },
                max_temp: { bsonType: "double" },
                rainfall: { bsonType: "double" },
                humidity_9am: { bsonType: "double" },
                humidity_3pm: { bsonType: "double" },
                pressure_9am: { bsonType: "double" },
                pressure_3pm: { bsonType: "double" },
                wind_speed_9am: { bsonType: "double" },
                wind_speed_3pm: { bsonType: "double" },
                wind_dir_9am: { bsonType: "string" },
                wind_dir_3pm: { bsonType: "string" },
                cloud_9am: { bsonType: "double" },
                cloud_3pm: { bsonType: "double" },
                temp_9am: { bsonType: "double" },
                temp_3pm: { bsonType: "double" },
                rain_today: { bsonType: "bool" },
                rain_tomorrow: { bsonType: "bool" }
            }
        }
    }
});

db.createCollection("rain_predictions", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["observation_id", "will_it_rain", "predicted_at"],
            properties: {
                observation_id: {
                    bsonType: "objectId",
                    description: "Reference to weather_observations collection"
                },
                will_it_rain: {
                    bsonType: "bool",
                    description: "Prediction result"
                },
                predicted_at: {
                    bsonType: "date",
                    description: "Timestamp of prediction"
                }
            }
        }
    }
});

// Indexes for performance and data integrity
// Locations collection
db.locations.createIndex({ "name": 1 }, { unique: true });

// Weather observations collection
db.weather_observations.createIndex({ "location_id": 1 }, { 
    name: "location_reference",
    background: true 
});
db.weather_observations.createIndex({ "date": 1 }, { 
    name: "date_lookup",
    background: true 
});
db.weather_observations.createIndex({ "location_id": 1, "date": -1 }, { 
    name: "location_date_lookup",
    background: true 
});

// Rain predictions collection
db.rain_predictions.createIndex({ "observation_id": 1 }, { 
    name: "observation_reference",
    background: true 
});
db.rain_predictions.createIndex({ "predicted_at": -1 }, { 
    name: "prediction_time_lookup",
    background: true 
});
db.rain_predictions.createIndex({ "observation_id": 1, "predicted_at": -1 }, {
    name: "observation_time_lookup",
    background: true
});

// Time-to-live (TTL) index for predictions (optional, adjust expireAfterSeconds as needed)
db.rain_predictions.createIndex({ "predicted_at": 1 }, { 
    expireAfterSeconds: 7776000,  // 90 days
    name: "prediction_ttl" 
});
