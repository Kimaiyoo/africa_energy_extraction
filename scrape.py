import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from pymongo import MongoClient
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

class AfricaEnergyPortalScraper:
    def __init__(self, mongo_uri=None, db_name="AfricaEnergyData"):
        """Initialize scraper with MongoDB connection"""
        self.base_url = "https://africa-energy-portal.org"
        self.driver = None
        self.mongo_client = None
        self.db = None
        self.collection = None
        
        if mongo_uri:
            self.mongo_client = MongoClient(mongo_uri)
            self.db = self.mongo_client[db_name]
            self.collection = self.db['energy_metrics']
            logging.info(f"Connected to MongoDB: {db_name}")
        
        self.start_year = 2000
        self.end_year = 2024
        self.years = list(range(self.start_year, self.end_year + 1))
        
        # Track data quality
        self.missing_data_log = []
        
    def setup_driver(self):
        """Setup Selenium WebDriver with Chrome options"""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Run in background
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.maximize_window()
        logging.info("WebDriver initialized")
    
    def get_african_countries(self):
        """Return list of all African countries"""
        countries = [
            "Algeria", "Angola", "Benin", "Botswana", "Burkina Faso", "Burundi",
            "Cabo Verde", "Cameroon", "Central African Republic", "Chad", "Comoros",
            "Congo", "Democratic Republic of the Congo", "Djibouti", "Egypt",
            "Equatorial Guinea", "Eritrea", "Eswatini", "Ethiopia", "Gabon",
            "Gambia", "Ghana", "Guinea", "Guinea-Bissau", "Ivory Coast", "Kenya",
            "Lesotho", "Liberia", "Libya", "Madagascar", "Malawi", "Mali",
            "Mauritania", "Mauritius", "Morocco", "Mozambique", "Namibia",
            "Niger", "Nigeria", "Rwanda", "Sao Tome and Principe", "Senegal",
            "Seychelles", "Sierra Leone", "Somalia", "South Africa", "South Sudan",
            "Sudan", "Tanzania", "Togo", "Tunisia", "Uganda", "Zambia", "Zimbabwe"
        ]
        return countries
    
    def scrape_country_data(self, country):
        """Scrape energy data for a specific country"""
        logging.info(f"Scraping data for {country}")
        country_data = []
        
        try:
            # Navigate to country page
            country_url = f"{self.base_url}/countries/{country.lower().replace(' ', '-')}"
            self.driver.get(country_url)
            time.sleep(3)  # Wait for page load
            
            # Extract available metrics
            metrics = self.extract_metrics(country)
            
            return metrics
            
        except Exception as e:
            logging.error(f"Error scraping {country}: {str(e)}")
            self.missing_data_log.append({
                'country': country,
                'error': str(e),
                'timestamp': datetime.now()
            })
            return []
    
    def extract_metrics(self, country):
        """Extract all energy metrics for a country"""
        metrics = []
        
        try:
            # Wait for data containers to load
            wait = WebDriverWait(self.driver, 10)
            
            # Find all data sections (this needs to be adjusted based on actual site structure)
            data_sections = self.driver.find_elements(By.CLASS_NAME, 'data-section')
            
            for section in data_sections:
                try:
                    # Extract metric details
                    metric_name = section.find_element(By.CLASS_NAME, 'metric-name').text
                    unit = section.find_element(By.CLASS_NAME, 'unit').text if section.find_elements(By.CLASS_NAME, 'unit') else "N/A"
                    
                    # Extract yearly data
                    yearly_data = {}
                    for year in self.years:
                        try:
                            year_elem = section.find_element(By.CSS_SELECTOR, f'[data-year="{year}"]')
                            value = year_elem.text
                            yearly_data[str(year)] = self.parse_value(value)
                        except NoSuchElementException:
                            yearly_data[str(year)] = None
                            self.missing_data_log.append({
                                'country': country,
                                'metric': metric_name,
                                'year': year,
                                'reason': 'Data not available'
                            })
                    
                    # Build metric record
                    metric_record = {
                        'country': country,
                        'country_serial': self.get_country_serial(country),
                        'metric': metric_name,
                        'unit': unit,
                        'sector': self.extract_sector(metric_name),
                        'sub_sector': self.extract_sub_sector(metric_name),
                        'sub_sub_sector': self.extract_sub_sub_sector(metric_name),
                        'source_link': self.driver.current_url,
                        'source': 'Africa Energy Portal',
                        **yearly_data  # Add all year columns
                    }
                    
                    metrics.append(metric_record)
                    
                except Exception as e:
                    logging.warning(f"Error extracting metric in {country}: {str(e)}")
                    continue
            
            logging.info(f"Extracted {len(metrics)} metrics for {country}")
            return metrics
            
        except TimeoutException:
            logging.error(f"Timeout while loading data for {country}")
            return []
    
    def parse_value(self, value_str):
        """Parse value string to appropriate numeric type"""
        if not value_str or value_str.strip() in ['', '-', 'N/A']:
            return None
        
        try:
            # Remove common formatting
            clean_value = value_str.replace(',', '').replace('%', '').strip()
            
            # Try converting to float
            if '.' in clean_value:
                return float(clean_value)
            else:
                return int(clean_value)
        except ValueError:
            return value_str  # Return as string if not numeric
    
    def get_country_serial(self, country):
        """Generate country serial number"""
        countries = self.get_african_countries()
        try:
            return countries.index(country) + 1
        except ValueError:
            return 999  # Unknown country
    
    def extract_sector(self, metric_name):
        """Extract sector from metric name"""
        metric_lower = metric_name.lower()
        
        if any(word in metric_lower for word in ['electricity', 'power', 'generation']):
            return 'Electricity'
        elif any(word in metric_lower for word in ['renewable', 'solar', 'wind', 'hydro']):
            return 'Renewables'
        elif any(word in metric_lower for word in ['access', 'electrification']):
            return 'Access'
        elif any(word in metric_lower for word in ['consumption', 'demand']):
            return 'Consumption'
        elif any(word in metric_lower for word in ['capacity', 'installed']):
            return 'Capacity'
        else:
            return 'General'
    
    def extract_sub_sector(self, metric_name):
        """Extract sub-sector from metric name"""
        metric_lower = metric_name.lower()
        
        if 'solar' in metric_lower:
            return 'Solar'
        elif 'wind' in metric_lower:
            return 'Wind'
        elif 'hydro' in metric_lower:
            return 'Hydro'
        elif 'fossil' in metric_lower or 'coal' in metric_lower:
            return 'Fossil Fuels'
        elif 'rural' in metric_lower:
            return 'Rural'
        elif 'urban' in metric_lower:
            return 'Urban'
        else:
            return 'N/A'
    
    def extract_sub_sub_sector(self, metric_name):
        """Extract sub-sub-sector from metric name"""
        # This can be further refined based on actual data structure
        return 'N/A'
    
    def save_to_mongodb(self, data):
        """Save data to MongoDB collection"""
        if not self.collection:
            logging.warning("MongoDB not configured. Skipping save.")
            return
        
        try:
            if data:
                result = self.collection.insert_many(data)
                logging.info(f"Inserted {len(result.inserted_ids)} documents to MongoDB")
                return True
            else:
                logging.warning("No data to save")
                return False
        except Exception as e:
            logging.error(f"Error saving to MongoDB: {str(e)}")
            return False
    
    def save_to_csv(self, data, filename='africa_energy_data.csv'):
        """Save data to CSV file"""
        try:
            df = pd.DataFrame(data)
            
            # Ensure column order
            base_cols = ['country', 'country_serial', 'metric', 'unit', 'sector', 
                        'sub_sector', 'sub_sub_sector', 'source_link', 'source']
            year_cols = [str(year) for year in self.years]
            all_cols = base_cols + year_cols
            
            # Reorder columns
            df = df[all_cols]
            
            df.to_csv(filename, index=False)
            logging.info(f"Data saved to {filename}")
            return True
        except Exception as e:
            logging.error(f"Error saving to CSV: {str(e)}")
            return False
    
    def generate_quality_report(self):
        """Generate data quality and completeness report"""
        report = {
            'total_missing_entries': len(self.missing_data_log),
            'missing_by_country': {},
            'missing_by_year': {},
            'missing_by_metric': {}
        }
        
        # Analyze missing data
        for entry in self.missing_data_log:
            country = entry.get('country')
            year = entry.get('year')
            metric = entry.get('metric')
            
            if country:
                report['missing_by_country'][country] = report['missing_by_country'].get(country, 0) + 1
            if year:
                report['missing_by_year'][str(year)] = report['missing_by_year'].get(str(year), 0) + 1
            if metric:
                report['missing_by_metric'][metric] = report['missing_by_metric'].get(metric, 0) + 1
        
        # Save report
        with open('data_quality_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logging.info("Quality report generated: data_quality_report.json")
        return report
    
    def run(self):
        """Main execution method"""
        logging.info("Starting Africa Energy Portal data extraction")
        
        try:
            self.setup_driver()
            
            all_data = []
            countries = self.get_african_countries()
            
            for i, country in enumerate(countries, 1):
                logging.info(f"Processing {i}/{len(countries)}: {country}")
                
                country_metrics = self.scrape_country_data(country)
                all_data.extend(country_metrics)
                
                # Save incrementally every 10 countries
                if i % 10 == 0:
                    self.save_to_csv(all_data, f'africa_energy_data_checkpoint_{i}.csv')
                
                time.sleep(2)  # Be respectful to the server
            
            # Final save
            logging.info(f"Total records collected: {len(all_data)}")
            self.save_to_csv(all_data)
            
            if self.collection:
                self.save_to_mongodb(all_data)
            
            # Generate quality report
            self.generate_quality_report()
            
            logging.info("Data extraction completed successfully!")
            
        except Exception as e:
            logging.error(f"Critical error during execution: {str(e)}")
        
        finally:
            if self.driver:
                self.driver.quit()
            if self.mongo_client:
                self.mongo_client.close()


if __name__ == "__main__":
    # Configuration
    MONGO_URI = "your_mongodb_connection_string_here"  # Replace with actual URI
    DB_NAME = "AfricaEnergyData"
    
    # Run scraper
    scraper = AfricaEnergyPortalScraper(mongo_uri=MONGO_URI, db_name=DB_NAME)
    scraper.run()