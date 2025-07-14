# upload_to_gcs.py

import os
import logging
from google.cloud import storage

os.makedirs("logs", exist_ok=True)

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


   

