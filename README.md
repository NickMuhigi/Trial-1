# Weather Data Database & API Project

## Overview
This project demonstrates a comprehensive weather data pipeline using both PostgreSQL (Supabase) and MongoDB databases. It includes a FastAPI application for data access, data migration tools, and robust data import capabilities.

## Features
- Dual Database System:
  - Relational schema (PostgreSQL/Supabase)
  - NoSQL schema (MongoDB) with validation rules
- RESTful API with FastAPI
  - CRUD operations for locations, observations, and predictions
  - Support for both PostgreSQL and MongoDB operations
  - Detailed response messages and error handling
- Data Management:
  - CSV data import with chunked processing
  - Database migration tools
  - Data validation and verification
- Schema Documentation:
  - ERD diagram for PostgreSQL schema
  - MongoDB collection schemas with validation rules
  - Complete API documentation

## Project Structure
```
.
├── api/
│   ├── __init__.py
│   ├── database.py    # Database connection management
│   ├── models.py      # Pydantic models for data validation
│   └── weather_api.py # FastAPI application endpoints
├── config/
│   ├── mongodb_collections_example.js # MongoDB schema and indexes
│   └── weather_erd.dbml              # PostgreSQL ERD definition
├── data/
│   ├── weatherAUS.csv          # Weather dataset
│   └── import_weather_data.py  # Data import script
├── migrations/
│   ├── migrate_to_mongodb.py   # PostgreSQL to MongoDB migration
│   └── verify_migration.py     # Migration verification tool
├── .env            # Environment variables (create from template)
├── requirements.txt # Python dependencies
└── README.md       # Project documentation
```

## Setup

### 1. Environment Setup
```bash
# Create and activate virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Configuration
1. Copy `.env.example` to `.env` (create if needed)
2. Configure your database connections in `.env`:
   ```
   # PostgreSQL
   PG_HOST=your-host
   PG_PORT=your-port
   PG_DBNAME=your-db
   PG_USER=your-user
   PG_PASSWORD=your-password

   # MongoDB
   MONGO_URI=your-mongodb-uri
   MONGO_DB=weather_db
   ```

### 3. Database Initialization
1. Set up PostgreSQL schema:
   - Use `config/weather_erd.dbml` for reference
   - Create tables using your preferred PostgreSQL tool

2. Set up MongoDB schema:
   - Run the schema definitions from `config/mongodb_collections_example.js`

### 4. Data Import
```bash
# Import weather data into both databases
cd data
python import_weather_data.py
```

### 5. Run the API
```bash
# From the project root
uvicorn api.weather_api:app --reload
```
- Access the API documentation at http://127.0.0.1:8000/docs
- Interactive OpenAPI documentation at http://127.0.0.1:8000/redoc

## API Endpoints

### PostgreSQL Endpoints
- `/locations/` - CRUD operations for locations
- `/observations/` - CRUD operations for weather observations
- `/predictions/` - CRUD operations for rain predictions

### MongoDB Endpoints
- `/mongo/locations/` - CRUD operations for MongoDB locations
- `/mongo/observations/` - CRUD operations for MongoDB weather observations
- `/mongo/predictions/` - CRUD operations for MongoDB rain predictions

Each endpoint supports:
- GET (list & detail)
- POST (create)
- PUT (update)
- DELETE (remove)

## Database Migration

To migrate data from PostgreSQL to MongoDB:
```bash
cd migrations
python migrate_to_mongodb.py
python verify_migration.py  # Verify the migration
```

## Notes
- The API uses sequential IDs for all collections in MongoDB
- Data validation is enforced at both API and database levels
- All timestamps are stored in UTC
- Batch operations use configurable chunk sizes for performance
- Error handling includes detailed messages and proper HTTP status codes

---

## ERD IMAGE

![Weather ERD][imageRef]

[imageRef]: docs/Untitled.png


**Author:** Group 15
