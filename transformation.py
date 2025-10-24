import pandas as pd
import re
import os

# Configuration
DATASETS = [
    "datasets/energy.xlsx",
    "datasets/electricity.xlsx",
    "datasets/social_and_economic.xlsx"
]

SOURCE_LINK = "https://africa-energy-portal.org/"
SOURCE_NAME = "Africa Energy Portal"

# Helper Functions
def detect_sector_logic(dataset_name, indicator):
    # Returns sub_sector and sub_sub_sector depending on the dataset and indicator text.
    indicator_lower = indicator.lower()

    if "energy" in dataset_name:
        # Energy logic
        if "access" in indicator_lower:
            sub_sector = "Access"
        elif "efficiency" in indicator_lower or "intensity" in indicator_lower:
            sub_sector = "Efficiency"
        else:
            sub_sector = "Other"
        sub_sub_sector = indicator.strip()

    elif "electricity" in dataset_name:
        # Electricity logic
        if "access" in indicator_lower:
            sub_sector = "Access"
            sub_sub_sector = indicator.strip()
        elif "installed capacity" in indicator_lower:
            sub_sector = "Technical"
            sub_sub_sector = indicator.strip()
        else:
            sub_sector = "Supply"
            sub_sub_sector = indicator.strip()

    elif "social" in dataset_name:
        # Social and Economic logic
        if "gdp" in indicator_lower:
            sub_sector = "National Account"
            sub_sub_sector = indicator.strip()
        else:
            sub_sector = "Population"
            sub_sub_sector = indicator.strip()

    else:
        sub_sector, sub_sub_sector = "Unknown", "Unknown"

    return sub_sector, sub_sub_sector


def extract_unit_and_metric(indicator, dataset_name):
    # Extracts the unit (inside parentheses) and the metric (cleaned text).
    unit_match = re.search(r"\((.*?)\)", indicator)
    unit = unit_match.group(1) if unit_match else ""
    metric = re.sub(r"\s*\(.*?\)", "", indicator)
    metric = metric.replace("Energy:", "").replace("Electricity:", "").strip()
    return metric, unit


def process_dataset(file_path):
    #Process a single dataset file.
    dataset_name = os.path.basename(file_path).lower().replace(".xlsx", "")
    print(f"\n Processing {dataset_name} dataset...")

    excel_data = pd.ExcelFile(file_path)
    all_data = []

    for sheet_name in excel_data.sheet_names:
        sector = dataset_name.replace("_", " ")
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        df.columns = df.columns.str.strip()

        # Validation and cleaning
        missing_count = df.isna().sum().sum()

        if missing_count > 0:
            print(f"Found {missing_count} missing values in '{sheet_name}'... replacing numeric NaNs with 0.")

            missing_by_col = df.isna().sum()
            missing_by_col = missing_by_col[missing_by_col > 0]
            print(missing_by_col.to_string())

            for col in df.select_dtypes(include=["float", "int"]).columns:
                df[col] = df[col].fillna(0)
            
            missing_verification = df.isna().sum().sum()
            if missing_verification == 0:
                print("No missing values")
            else:
                print(f"found {missing_verification} missing values.")

        else:
            print(f"No missing values in '{sheet_name}'.")

        for _, row in df.iterrows():
            country = row["Country"]
            indicator = row["Indicator"]

            sub_sector, sub_sub_sector = detect_sector_logic(dataset_name, indicator)
            metric, unit = extract_unit_and_metric(indicator, dataset_name)

            # Extract yearly values
            year_values = {
                str(year): row.get(str(year), None)
                for year in range(2000, 2023)
            }

            record = {
                "country": country,
                "metric": metric,
                "unit": unit,
                "sector": sector,
                "sub_sector": sub_sector,
                "sub_sub_sector": sub_sub_sector,
                "source_link": SOURCE_LINK,
                "source": SOURCE_NAME,
                **year_values
            }

            all_data.append(record)

    # Convert to DataFrame
    df = pd.DataFrame(all_data)

    # Add country_serial
    unique_countries = df["country"].unique()
    country_serial_map = {country: i + 1 for i, country in enumerate(unique_countries)}
    df["country_serial"] = df["country"].map(country_serial_map)

    # Reorder columns
    year_columns = [str(year) for year in range(2000, 2023)]
    column_order = [
        "country",
        "country_serial",
        "metric",
        "unit",
        "sector",
        "sub_sector",
        "sub_sub_sector",
        "source_link",
        "source",
    ] + year_columns

    df = df[column_order]


    # Save outputs
    output_name = dataset_name
    df.to_csv(f"datasets/{output_name}.csv", index=False)
    
    print(f" {dataset_name.capitalize()} dataset formatted successfully.")
    return df


# Main Execution
if __name__ == "__main__":
    for dataset in DATASETS:
        process_dataset(dataset)

    print("\n All datasets formatted and saved to 'datasets/' folder.")
