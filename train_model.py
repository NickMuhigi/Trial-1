import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import joblib
import os

df = pd.read_csv("data/weatherAUS.csv")


df.dropna(subset=["RainTomorrow"], inplace=True)


df["RainTomorrow"] = df["RainTomorrow"].map({"Yes": 1, "No": 0})


features = [
    "MinTemp", "MaxTemp", "Rainfall", "WindGustSpeed",
    "Humidity9am", "Humidity3pm", "Pressure9am", "Pressure3pm",
    "Cloud3pm", "Temp3pm"
]

df = df.dropna(subset=features)
X = df[features]
y = df["RainTomorrow"]


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


model = RandomForestClassifier(n_estimators=10, random_state=42)
model.fit(X_train, y_train)

preds = model.predict(X_test)
accuracy = accuracy_score(y_test, preds)
print(f"Model Accuracy: {accuracy:.2f}")


os.makedirs("models", exist_ok=True)
joblib.dump(model, "models/rain_prediction_model.pkl")
print("Model saved to models/rain_prediction_model.pkl")
