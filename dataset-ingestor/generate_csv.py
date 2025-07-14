import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/create_csv.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("Starting Sentiment140 processing script.")

df = pd.read_csv("training.1600000.processed.noemoticon.csv", encoding="ISO-8859-1", header=None)
df.columns = ["target", "id", "date", "query", "user", "text"]

logger.info("Mapping sentiment labels and parsing timestamps.")
df["sentiment"] = df["target"].map({0: "negative", 2: "neutral", 4: "positive"})
df["timestamp"] = pd.to_datetime(df["date"], errors='coerce')

df_out = df[["id", "text", "sentiment", "timestamp"]]

parquet_path = "sentiment140.parquet"
csv_path = "sentiment140_processed.csv"

if not os.path.exists(parquet_path):
    df_out.to_parquet(parquet_path)
    logger.info(f"Created Parquet file: {parquet_path}")
else:
    logger.info(f"{parquet_path} already exists. Skipping Parquet creation.")

if not os.path.exists(csv_path):
    df_out.to_csv(csv_path, index=False)
    logger.info(f"Created CSV file: {csv_path}")
else:
    logger.info(f"{csv_path} already exists. Skipping CSV creation.")

logger.info("Processing complete.")
