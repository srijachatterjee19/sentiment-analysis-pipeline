import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Read ENV from .env
ENV = os.getenv("ENV", "prod")


os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/spark_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

logger.info("🚀 Starting Spark Job...")

spark = SparkSession.builder \
    .appName("FixTimestamp") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
    .getOrCreate()

logger.info("✅ SparkSession initialized.")

spark.conf.set("credentialsFile", "key.json")

if ENV == "local":
    logger.info("🔧 LOCAL mode: loading Parquet")
    df = spark.read.option("header", "true").csv("../dataset-ingestor/sentiment140_processed.csv")
else:
    logger.info("🚀 PROD mode: loading BigQuery")
    
    try:
        df = spark.read.format("bigquery") \
            .option("table", "sentiment-pipeline-465912.sentimentanalysisdatabase.posts") \
            .load()
    except Exception as e:
        logger.error(f"❌ Failed to load from BigQuery: {e}")
    raise

logger.info(f"✅ Read {df.count()} rows from Dataset.")

logger.info("✅Done reading from dataset")

logger.info("Showing dataset schema");
logger.info(df.columns)

rows = df.take(5)

for i, row in enumerate(rows):
    logger.info(f"Row {i+1}: {row.asDict()}")


logger.info("📊 Grouping by date and sentiment...")
result = df.groupBy("timestamp", "sentiment").count().orderBy("timestamp")
logger.info(f"✅ Grouped data: {result.count()} rows.")
result.show(truncate=False)

logger.info("🎉 Spark job completed successfully.")