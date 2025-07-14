import os
import pandas as pd
from google.cloud import storage, bigquery
import zipfile
import subprocess
import logging
from dotenv import load_dotenv

load_dotenv()

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Paths & Configs
parquet_path = "sentiment140.parquet"
gcs_blob_name = "sentiment140.parquet"
bucket_name = os.environ["GCS_BUCKET"]
dataset_id = "sentiment-pipeline-465912.sentimentanalysisdatabase"
table_id = f"{dataset_id}.posts"

# ✅ Convert CSV → Parquet only if parquet doesn't exist
if os.path.exists(parquet_path):
    logger.info(f"🟡 {parquet_path} already exists. Skipping CSV conversion.")
else:
    logger.info(f"📄 Reading and converting CSV → Parquet...")
    df = pd.read_csv(csv_path, encoding="ISO-8859-1", header=None)
    df.columns = ["target", "id", "date", "query", "user", "text"]
    df["sentiment"] = df["target"].map({0: "negative", 2: "neutral", 4: "positive"})
    df["timestamp"] = pd.to_datetime(df["date"], errors='coerce')
    df_out = df[["id", "text", "sentiment", "timestamp"]]
    df_out.to_parquet(parquet_path)
    logger.info(f"✅ Parquet written to {parquet_path}")


# ✅ GCS Upload (if not already present)
logger.info(f"Checking if {gcs_blob_name} exists in GCS bucket: {bucket_name}...")
gcs_client = storage.Client()
bucket = gcs_client.bucket(bucket_name)
blob = bucket.blob(gcs_blob_name)

if blob.exists():
    logger.info(f"🟡 Blob '{gcs_blob_name}' already exists in GCS. Skipping upload.")
else:
    logger.info(f"⬆️ Uploading {parquet_path} to GCS bucket: {bucket_name}...")
    blob.upload_from_filename(parquet_path)
    logger.info("✅ Upload to GCS successful.")

# ✅ BigQuery Upload (if table doesn't exist)
logger.info(f"Checking if BigQuery table {table_id} exists...")
bq_client = bigquery.Client()

try:
    bq_client.get_table(table_id)
    logger.info(f"🟡 Table `{table_id}` already exists. Skipping BigQuery upload.")
except Exception as e:
    logger.info(f"⬆️ Table does not exist. Uploading {parquet_path} to BigQuery...")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True
    )
    with open(parquet_path, "rb") as f:
        load_job = bq_client.load_table_from_file(f, table_id, job_config=job_config)
    load_job.result()
    logger.info(f"✅ Uploaded {parquet_path} to BigQuery table `{table_id}`.")