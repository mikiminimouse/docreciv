"""
Configuration for the Sync DB microservice.
"""
import os
from datetime import timedelta

# MongoDB settings for remote connection (through VPN)
REMOTE_MONGO_URI = os.getenv("MONGO_SERVER", "192.168.0.46:8635")
REMOTE_MONGO_USER = os.getenv("MONGO_USER", "readProtocols223")
REMOTE_MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
REMOTE_MONGO_SSL_CERT = os.getenv("MONGO_SSL_CERT", "/root/winners_preprocessor/receiver/certs/sber2.crt")

# MongoDB settings for local connection
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_SERVER", "localhost:27017")
LOCAL_MONGO_USER = os.getenv("MONGO_METADATA_USER", "docling_user")
LOCAL_MONGO_PASSWORD = os.getenv("MONGO_METADATA_PASSWORD")
LOCAL_MONGO_DB = os.getenv("MONGO_METADATA_DB", "docling_metadata")

# Synchronization parameters
FULL_SYNC_DAYS = 14  # Full synchronization for 2 weeks
BATCH_SIZE = 1000    # Batch size for processing
MAX_WORKERS = 4      # Maximum number of threads for parallel processing

# Collection names
REMOTE_COLLECTION = "protocols223.purchaseProtocol"
LOCAL_COLLECTION = "docling_metadata.protocols"
