from pydantic import BaseModel, Field
from typing import Optional, Union
from datetime import date, datetime

# PostgreSQL Models
class LocationBase(BaseModel):
    name: str
    state: Optional[str] = None

class Location(LocationBase):
    location_id: int

class ObservationBase(BaseModel):
    location_id: int
    date: date
    min_temp: Optional[float] = None
    max_temp: Optional[float] = None
    rainfall: Optional[float] = None
    humidity_9am: Optional[float] = None
    humidity_3pm: Optional[float] = None
    pressure_9am: Optional[float] = None
    pressure_3pm: Optional[float] = None
    wind_speed_9am: Optional[float] = None
    wind_speed_3pm: Optional[float] = None
    wind_dir_9am: Optional[str] = None
    wind_dir_3pm: Optional[str] = None
    cloud_9am: Optional[float] = None
    cloud_3pm: Optional[float] = None
    temp_9am: Optional[float] = None
    temp_3pm: Optional[float] = None
    rain_today: Optional[bool] = None
    rain_tomorrow: Optional[bool] = None

class Observation(ObservationBase):
    observation_id: int

class PredictionBase(BaseModel):
    observation_id: int
    will_it_rain: bool

class Prediction(PredictionBase):
    prediction_id: int
    predicted_at: datetime

# MongoDB Models
class MongoLocationBase(BaseModel):
    name: str
    state: Optional[str] = None

class MongoLocation(MongoLocationBase):
    location_id: int  # Sequential ID only for locations

class MongoObservationBase(BaseModel):
    location_id: int  # References mongo_id from locations
    date: Union[date, datetime]
    min_temp: Optional[float] = None
    max_temp: Optional[float] = None
    rainfall: Optional[float] = None
    humidity_9am: Optional[float] = None
    humidity_3pm: Optional[float] = None
    pressure_9am: Optional[float] = None
    pressure_3pm: Optional[float] = None
    wind_speed_9am: Optional[float] = None
    wind_speed_3pm: Optional[float] = None
    wind_dir_9am: Optional[str] = None
    wind_dir_3pm: Optional[str] = None
    cloud_9am: Optional[float] = None
    cloud_3pm: Optional[float] = None
    temp_9am: Optional[float] = None
    temp_3pm: Optional[float] = None
    rain_today: Optional[bool] = None
    rain_tomorrow: Optional[bool] = None

class MongoObservation(MongoObservationBase):
    observation_id: int  # Sequential ID for observations

class MongoPredictionBase(BaseModel):
    observation_id: int  # References observation_id, not MongoDB _id
    will_it_rain: bool
    predicted_at: datetime = Field(default_factory=datetime.utcnow)

class MongoPrediction(MongoPredictionBase):
    prediction_id: int  # Sequential ID for predictions
    location_id: int  # Added to track which location this prediction belongs to
