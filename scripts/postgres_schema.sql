-- PostgreSQL Schema for Weather Data

CREATE TABLE locations (
  location_id SERIAL PRIMARY KEY
  , name VARCHAR(100) NOT NULL
  , state VARCHAR(50)
);

CREATE TABLE weather_observations (
  observation_id SERIAL PRIMARY KEY
  , location_id INTEGER REFERENCES locations(location_id)
  , date DATE
  , min_temp FLOAT
  , max_temp FLOAT
  , rainfall FLOAT
  , humidity_9am FLOAT
  , humidity_3pm FLOAT
  , pressure_9am FLOAT
  , pressure_3pm FLOAT
  , wind_speed_9am FLOAT
  , wind_speed_3pm FLOAT
  , wind_dir_9am VARCHAR(10)
  , wind_dir_3pm VARCHAR(10)
  , cloud_9am FLOAT
  , cloud_3pm FLOAT
  , temp_9am FLOAT
  , temp_3pm FLOAT
  , rain_today BOOLEAN
  , rain_tomorrow BOOLEAN
);

CREATE TABLE rain_predictions (
  prediction_id SERIAL PRIMARY KEY
  , observation_id INTEGER REFERENCES weather_observations(observation_id)
  , will_it_rain BOOLEAN
  , predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Stored Procedure Example
CREATE OR REPLACE FUNCTION insert_weather_observation(
  loc_id INTEGER
  , obs_date DATE
  , min_t FLOAT
  , max_t FLOAT
  , rain FLOAT
)
RETURNS VOID AS $$
BEGIN
  INSERT INTO
    weather_observations(location_id, date, min_temp, max_temp, rainfall)
  VALUES
    (loc_id, obs_date, min_t, max_t, rain);
END;
$$
LANGUAGE plpgsql;

-- Trigger Example
CREATE TABLE weather_observation_logs (
  log_id SERIAL PRIMARY KEY
  , observation_id INTEGER
  , changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  , old_min_temp FLOAT
  , new_min_temp FLOAT
);

CREATE OR REPLACE FUNCTION log_min_temp_change()
RETURNS TRIGGER AS $$
BEGIN
  IF
    NEW.min_temp IS DISTINCT FROM
    OLD.min_temp
  THEN
    INSERT INTO
      weather_observation_logs(observation_id, old_min_temp, new_min_temp)
    VALUES
      (
        OLD.observation_id
        , OLD.min_temp
        , NEW.min_temp
      );
  END IF;
  RETURN
  NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trg_min_temp_change
AFTER UPDATE
ON weather_observations
FOR EACH ROW EXECUTE FUNCTION log_min_temp_change();