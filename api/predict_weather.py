import requests
import joblib
import pandas as pd

# Step 1: Fetch the latest weather observation from your FastAPI
response = requests.get("http://127.0.0.1:8000/observations/")
data = response.json()

if not data:
    raise Exception("No weather data found in the API.")

latest_entry = data[-1]  # Get the most recent entry

# Step 2: Prepare the features used by the model
features = [
    "MinTemp", "MaxTemp", "Rainfall", "WindGustSpeed",
    "Humidity9am", "Humidity3pm", "Pressure9am", "Pressure3pm",
    "Cloud3pm", "Temp3pm"
]

input_data = {key: latest_entry.get(key, 0) for key in features}
df = pd.DataFrame([input_data])

# Step 3: Load the trained model
model = joblib.load("models/rain_prediction_model.pkl")

# Step 4: Make prediction
prediction = model.predict(df)[0]
label = "Rain" if prediction == 1 else "No Rain"

print("Latest weather entry:", input_data)
print("Prediction:", label)
