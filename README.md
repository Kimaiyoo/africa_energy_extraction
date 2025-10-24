# 🌍 Africa Energy Data Extraction (2000–2022)

This project automates the extraction of energy-related datasets from the Africa Energy Portal
 using Playwright.
It collects indicators such as electricity generation, access, consumption, and renewables for all African countries (2000–2022), and prepares them for MongoDB storage and analysis.

## 🧩 Features

- Automated data scraping with Playwright
- Structured dataset formatting with consistent naming conventions
- Support for multiple categories (Electricity, Energy, Social & Economic)
- Automatic file download handling and timeout recovery
- Ready for integration with MongoDB

## 📂 Data Structure

Each dataset follows this schema:

```
["country", "country_serial", "metric", "unit", "sector",
 "sub_sector", "sub_sub_sector", "source_link", "source",
 "2000", "2001", ..., "2022"]
```
Each row represents one metric per country across all years.

## 🚀 Usage

1. Clone the repository
```bash
git clone https://github.com/Kimaiyo/africa_energy_extraction.git
cd africa_energy_extraction
```

2. Install dependencies
```bash
pip install -r requirements.txt
```
3. Install the required browsers
```bash
playwright install
```

4. Run the scraper
```bash
python scraper.py
```
5. Downloaded files will be saved in the datasets/ folder.

## ⚙️ Progress

- Implemented automated scraping and filtering logic
- Resolved initial timeout issue for Electricity dataset
- Files successfully downloaded and verified for multiple categories
- Next: Data formatting, validation, and MongoDB integration

## 🧠 Tech Stack
- Python 3
- Playwright
- MongoDB (upcoming integration)
