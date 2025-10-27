import pandas as pd
from pymongo import MongoClient
import os

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["energy_data"]

DATASETS = [
    "datasets/energy.csv",
    "datasets/electricity.csv",
    "datasets/social_and_economic.csv"
]

def load_csv_to_mongo(file_path):
    """Loads a CSV into MongoDB."""
    sector_name = os.path.basename(file_path).replace(".csv", "")
    collection = db[sector_name]

    print(f"\nLoading {sector_name} dataset...")

    # Ask whether to clear existing data
    choice = input(f"Do you want to clear existing data in '{sector_name}' collection? (y/n): ").strip().lower()
    if choice == "y":
        deleted = collection.delete_many({})
        print(f"Cleared {deleted.deleted_count} existing documents.")

    # Read CSV
    df = pd.read_csv(file_path)

    # Ensure correct data types
    for col in df.columns:
        if col.isdigit():
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Convert to dictionary
    data = df.to_dict(orient="records")

    # Insert into MongoDB
    if data:
        result = collection.insert_many(data)
        print(f"Inserted {len(result.inserted_ids)} documents into '{sector_name}' collection.")
    else:
        print(f"No data found in {file_path}")

# Main execution
if __name__ == "__main__":
    for dataset in DATASETS:
        load_csv_to_mongo(dataset)

    print("\nAll datasets loaded successfully into MongoDB.")
