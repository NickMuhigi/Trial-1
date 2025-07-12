# Database Migrations

This folder contains scripts for migrating data between different databases in the rainfall prediction system.

## Files

- `migrate_to_mongodb.py` - Script for migrating data from PostgreSQL to MongoDB
- `verify_migration.py` - Script for verifying that the data was correctly migrated to MongoDB

## Usage

1. First, run the migration:
   ```bash
   python migrate_to_mongodb.py
   ```

2. Then verify the migration:
   ```bash
   python verify_migration.py
   ```

Make sure both PostgreSQL and MongoDB databases are running before executing these scripts.

## Data Migration Process

1. The migration script (`migrate_to_mongodb.py`):
   - Connects to both PostgreSQL and MongoDB
   - Reads data from PostgreSQL tables
   - Transforms the data as needed
   - Writes the data to MongoDB collections
   - Maintains data relationships and integrity

2. The verification script (`verify_migration.py`):
   - Checks that all records were migrated
   - Verifies data integrity and relationships
   - Validates data types and formats
   - Reports any discrepancies found
