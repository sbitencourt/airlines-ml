# Flight Delay Prediction Pipeline: Technical Documentation

## 1. Data Source and Size
* **Data Provider:** EUROCONTROL (STATFOR Team).
* **Data Structure:** The data comes in quarterly packages. It covers four specific months of the year: March, June, September, and December to capture different seasons.
* **Dataset Volume:** * The total dataset size used for testing the model is **100,611 flights**. This is a 20% sample taken from the big main database.
  * The file containing the planned flight paths and coordinates (`Flight_Points_Filed_*.csv`) is **1.63 GB** in size.

## 2. Data Cleaning and Preparation Steps
We used PySpark in Databricks to clean and prepare the data before sending it to Scikit-Learn. We only used information known *before* the flight departed to make sure the model does not cheat (no data leakage):
1. **Creating the Target Link:** We calculated the delay by subtracting the planned departure time (`FILED OFF BLOCK TIME`) from the real departure time (`ACTUAL OFF BLOCK TIME`). If a flight was delayed by **more than 15 minutes**, we marked it as `1` (Delayed). Otherwise, it was marked as `0` (On-Time).
2. **Finding the Start Position:** We filtered the large 1.63 GB route points file to keep only the very first point of the flight (`Sequence Number == 0`). This gave us the exact starting Latitude and Longitude for each flight.
3. **Time Features:** We extracted the specific month, hour of the day, and day of the week from the flight dates. This helps the model understand airport rush hours and seasonal weather changes.
4. **Data Transformation:** We used Scikit-Learn's `ColumnTransformer` to automatically scale the numbers (like distance and altitude) using `StandardScaler`, and change text values (like `AC Operator`) into numbers using `OneHotEncoder`.

## 3. Chosen Features and Why
The model looks at these specific features to predict delays 2 hours before a flight departs:
* **`Latitude` & `Longitude`:** The exact location of the starting airport. This helps connect flights to local weather stations later.
* **`AC Operator`:** The airline code. Different airlines have different numbers of airplanes and operational speeds.
* **`Requested FL` & `Actual Distance Flown (nm)`:** The planned flight altitude and total distance. Longer or more complex routes have a higher risk of getting stuck in busy airspace.
* **`month`, `hour`, `day_of_week`:** These time values act as smart clues for bad winter weather or heavy airport traffic during rush hours.

## 4. Project and Data Limitations

### The Two-Year Delay
The major limitation of this dataset is that EUROCONTROL has a strict **two-year delay policy** before publishing their commercial flight data. For example, March 2024 is currently the newest data available. 

### Why We Cannot Use Real-Time APIs Right Now
Because of this two-year gap, we **cannot connect this model to live flight tracking APIs** to predict flights happening today:
* **Changes Over Time:** Airlines change their schedules, some companies go out of business, and flight routes change over two years. A model trained on 2024 data will make mistakes on 2026 live data.
* **Weather Sync:** To test this historical model, we must use matching historical weather data (like 2024 records from Météo-France) instead of today's live weather forecast.
* **Missing Months:** Because the data only includes 4 specific months, the model completely misses heavy summer holiday traffic spikes in July and August.

---

## 5. Preprocessing and Machine Learning Code

### Preprocessing and Train-Test Split
```python
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer

# Separate features array from targets vector
X = pandas_dataset.drop(columns=["is_delayed"])
y = pandas_dataset["is_delayed"]

# Configure structural encoding parameters
num_cols = ["Latitude", "Longitude", "Requested FL", "Actual Distance Flown (nm)", "month", "hour", "day_of_week"]
cat_cols = ["AC Operator"]

# Define operations inside a centralized ColumnTransformer
preprocessor = ColumnTransformer(
    transformers=[
        ('numeric_scaling', StandardScaler(), num_cols),
        ('categorical_encoding', OneHotEncoder(handle_unknown='ignore', sparse_output=False), cat_cols)
    ])

# Execute a stratified split to ensure identical delay proportions across sets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, 
    test_size=0.2, 
    random_state=84, 
    stratify=y
)

X_train_transformed = preprocessor.fit_transform(X_train)
X_test_transformed = preprocessor.transform(X_test)