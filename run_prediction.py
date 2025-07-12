import requests
import pandas as pd
import joblib

response = requests.get("http://127.0.0.1:8000/mongo/observations/")
if response.status_code == 200:
    data = response.json().get("data", [])
    if not data:
        print("No observations found.")
        exit()
    latest = max(data, key=lambda obs: obs["date"])
    print("Latest observation:", latest)
else:
    print("Failed to fetch data:", response.status_code)
    exit()

features = [
    "MinTemp", "MaxTemp", "Rainfall", "WindGustSpeed",
    "Humidity9am", "Humidity3pm", "Pressure9am", "Pressure3pm",
    "Cloud3pm", "Temp3pm"
]

input_data = {
    "MinTemp": latest.get("min_temp"),
    "MaxTemp": latest.get("max_temp"),
    "Rainfall": latest.get("rainfall"),
    "WindGustSpeed": latest.get("wind_speed_3pm"),
    "Humidity9am": latest.get("humidity_9am"),
    "Humidity3pm": latest.get("humidity_3pm"),
    "Pressure9am": latest.get("pressure_9am"),
    "Pressure3pm": latest.get("pressure_3pm"),
    "Cloud3pm": latest.get("cloud_3pm"),
    "Temp3pm": latest.get("temp_3pm")
}


missing_features = [k for k, v in input_data.items() if v is None]
if missing_features:
    print(f"Error: Missing feature data for {missing_features}")
    exit()


input_df = pd.DataFrame([input_data])

model = joblib.load("models/rain_prediction_model.pkl")

prediction = model.predict(input_df)[0]  


result = "Yes" if prediction == 1 else "No"
print(f"Prediction (Rain Tomorrow): {result}")
