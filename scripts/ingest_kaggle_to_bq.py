import os
import glob
import logging
import kagglehub
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# Configure standard logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_environment():
    """Loads environment variables securely."""
    load_dotenv()
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        logging.error("Missing GOOGLE_APPLICATION_CREDENTIALS in .env file.")
        raise EnvironmentError("GCP credentials not found.")

def download_kaggle_dataset() -> str:
    """Downloads the dataset and returns the path to the CSV file."""
    logging.info("Starting Kaggle download...")
    dataset_path = kagglehub.dataset_download("nelgiriyewithana/australian-vehicle-prices")
    
    # Locate the CSV within the downloaded directory
    csv_files = glob.glob(os.path.join(dataset_path, "*.csv"))
    if not csv_files:
        raise FileNotFoundError("No CSV file found in the downloaded Kaggle dataset.")
    
    csv_path = csv_files[0]
    logging.info(f"Dataset successfully located at: {csv_path}")
    return csv_path

def load_to_bigquery(csv_path: str):
    """Reads the CSV and loads it into BigQuery as the Bronze layer."""
    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = "aus_auto_market"  # Requires manual/TF setup in GCP beforehand
    table_name = "raw_vehicle_listings"
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    logging.info("Reading CSV into Pandas DataFrame...")
    # Ingest as string to bypass pandas type inference on dirty raw data
    df = pd.read_csv(csv_path, dtype=str)
    
    # Sanitize headers for BQ compatibility
    df.columns = [c.strip().replace(" ", "_").replace("/", "_") for c in df.columns]

    logging.info(f"Connecting to BigQuery. Target table: {table_id}")
    client = bigquery.Client()

    # BQ load job config
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", # Full refresh pattern
        autodetect=True # Schema inference for bronze layer
    )

    logging.info("Uploading data to BigQuery...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Block until upload completes

    table = client.get_table(table_id)
    logging.info(f"Success! Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}.")

def main():
    try:
        load_environment()
        csv_path = download_kaggle_dataset()
        load_to_bigquery(csv_path)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()