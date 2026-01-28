"""
Configuration for the Downloader Microservice.
"""
import os
from pathlib import Path

# MongoDB Configuration
MONGO_METADATA_DB = os.environ.get("MONGO_METADATA_DB", "docling_metadata")
MONGO_METADATA_PROTOCOLS_COLLECTION = os.environ.get(
    "MONGO_METADATA_PROTOCOLS_COLLECTION", "protocols"
)

# Download Configuration
MAX_URLS_PER_PROTOCOL = int(os.environ.get("MAX_URLS_PER_PROTOCOL", "15"))
DOWNLOAD_HTTP_TIMEOUT = int(os.environ.get("DOWNLOAD_HTTP_TIMEOUT", "120"))
DOWNLOAD_CONCURRENCY = int(os.environ.get("DOWNLOAD_CONCURRENCY", "20"))
PROTOCOLS_CONCURRENCY = int(os.environ.get("PROTOCOLS_CONCURRENCY", "20"))

# Directory Configuration
DEFAULT_INPUT_DIR = Path(os.environ.get("INPUT_DIR", "/root/winners_preprocessor/final_preprocessing/Data"))
PROCESSING_DATA_DIR = Path(os.environ.get("PROCESSING_DATA_DIR", "/home/pak/Processing data"))
