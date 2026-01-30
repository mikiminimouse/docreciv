"""
Enhanced Sync Service for protocol synchronization with improved error handling and logging.

This service handles the synchronization of procurement protocols from remote MongoDB
to local MongoDB with comprehensive error handling, logging, and monitoring capabilities.
"""

import os
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError, DuplicateKeyError

from ..core.config import get_config
from ..core.exceptions import ConnectionError, SyncError
from ..vpn_utils import ensure_vpn_connected, check_remote_mongo_vpn_access, get_vpn_status, is_openvpn_running, is_vpn_interface_up, check_vpn_routes

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler with formatting
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


@dataclass
class EnhancedSyncResult:
    """Enhanced result of synchronization with detailed metrics."""
    status: str
    message: str
    date: str
    scanned: int = 0
    inserted: int = 0
    skipped_existing: int = 0
    errors_count: int = 0
    duration: float = 0.0
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None
    statistics: Optional[Dict[str, Any]] = None

    @property
    def success(self) -> bool:
        """True if synchronization is successful or partially successful."""
        return self.status in ["success", "partial"]

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.statistics is None:
            self.statistics = {}


class EnhancedSyncService:
    """Enhanced service for synchronizing procurement protocols."""

    def __init__(self):
        """Initialize the enhanced sync service."""
        self.config = get_config()
        self.remote_client = None
        self.local_client = None
        self.logger = logger
        
        # Initialize counters for detailed statistics
        self.reset_counters()

    def reset_counters(self):
        """Reset all internal counters."""
        self.counters = {
            "single_url": 0,
            "multi_url": 0,
            "no_url": 0,
            "attachment_types": {},
            "processing_times": [],
            "error_types": {}
        }

    def _get_remote_mongo_client(self) -> Optional[MongoClient]:
        """
        Get MongoDB client for remote database with enhanced error handling.
        
        Returns:
            MongoClient for remote MongoDB or None if connection fails
        """
        try:
            remote_config = self.config.sync_db.remote_mongo
            
            # Validate required parameters
            required_params = [
                ("MONGO_SERVER", remote_config.server),
                ("MONGO_USER", remote_config.user),
                ("MONGO_PASSWORD", remote_config.password),
                ("MONGO_SSL_CERT", remote_config.ssl_cert)
            ]
            
            missing_params = [param[0] for param in required_params if not param[1]]
            if missing_params:
                self.logger.error(f"Missing remote MongoDB parameters: {missing_params}")
                return None

            # Check if SSL certificate exists
            if not os.path.exists(remote_config.ssl_cert):
                self.logger.error(f"SSL certificate not found: {remote_config.ssl_cert}")
                return None

            # Check VPN connection before connecting to remote MongoDB
            self.logger.info("Checking VPN connection for remote MongoDB access...")
            
            # Получаем детальный статус VPN
            vpn_status = get_vpn_status()
            
            # Логируем детальную информацию о VPN
            self.logger.info(f"VPN Status: OpenVPN running={vpn_status['openvpn_running']}, "
                           f"Interface up={vpn_status['interface_up']}, "
                           f"Overall status={vpn_status['overall_status']}")
            
            # Проверяем OpenVPN процесс
            if not vpn_status["openvpn_running"]:
                self.logger.error("OpenVPN process is not running. Please start OpenVPN first.")
                vpn_config = os.environ.get('VPN_CONFIG_FILE', '/root/winners_preprocessor/final_preprocessing/receiver/vitaly_bychkov.ovpn')
                self.logger.error(f"Example: sudo openvpn --config {vpn_config}")
                return None
            
            # Проверяем VPN интерфейс
            if not vpn_status["interface_up"]:
                self.logger.error("VPN interface (tun0/tap0) is not up. VPN tunnel may not be established.")
                return None
            
            # Проверяем маршруты к MongoDB серверу
            mongo_host = remote_config.server.split(':')[0]
            mongo_port = int(remote_config.server.split(':')[1]) if ':' in remote_config.server else 27017
            
            routes = check_vpn_routes([mongo_host])
            if not routes.get(mongo_host, False):
                self.logger.warning(f"Route to MongoDB server {mongo_host} not found through VPN interface.")
                self.logger.warning("This may indicate that route-up-zakupki.sh was not executed or VPN routes are not configured.")
            
            # Проверяем доступность MongoDB через VPN
            if not check_remote_mongo_vpn_access(mongo_host, mongo_port):
                self.logger.error(f"Remote MongoDB {mongo_host}:{mongo_port} is not accessible through VPN")
                self.logger.error("Please verify:")
                self.logger.error(f"  1. OpenVPN is running: {'✓' if vpn_status['openvpn_running'] else '✗'}")
                self.logger.error(f"  2. VPN interface is up: {'✓' if vpn_status['interface_up'] else '✗'}")
                self.logger.error(f"  3. Route to {mongo_host} exists: {'✓' if routes.get(mongo_host, False) else '✗'}")
                return None
            
            self.logger.info("VPN connection verified for remote MongoDB access")

            # Create connection URL
            url = remote_config.get_connection_url()
            
            self.logger.info(f"Connecting to remote MongoDB at {remote_config.server}")
            
            # Create client with SSL settings
            client = MongoClient(
                url,
                tls=True,
                tlsCAFile=remote_config.ssl_cert,
                tlsAllowInvalidHostnames=True,
                serverSelectionTimeoutMS=20000,
                connectTimeoutMS=10000,
                socketTimeoutMS=30000,
                maxPoolSize=10
            )
            
            # Test connection
            client.admin.command("ping")
            self.logger.info("Successfully connected to remote MongoDB")
            return client
            
        except ConnectionFailure as e:
            self.logger.error(f"Connection failure to remote MongoDB: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to remote MongoDB: {e}")
            return None

    def _get_local_mongo_client(self) -> Optional[MongoClient]:
        """
        Get MongoDB client for local database with enhanced error handling.
        
        Returns:
            MongoClient for local MongoDB or None if connection fails
        """
        try:
            local_config = self.config.sync_db.local_mongo
            
            # Validate required parameters
            required_params = [
                ("LOCAL_MONGO_SERVER", local_config.server),
                ("MONGO_METADATA_USER", local_config.user),
                ("MONGO_METADATA_PASSWORD", local_config.password)
            ]
            
            missing_params = [param[0] for param in required_params if not param[1]]
            if missing_params:
                self.logger.error(f"Missing local MongoDB parameters: {missing_params}")
                return None

            # Create connection URL
            url = local_config.get_connection_url()
            
            self.logger.info(f"Connecting to local MongoDB at {local_config.server}")
            
            # Create client
            client = MongoClient(
                url,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
                maxPoolSize=10
            )
            
            # Test connection
            client.admin.command("ping")
            self.logger.info("Successfully connected to local MongoDB")
            return client
            
        except ConnectionFailure as e:
            self.logger.error(f"Connection failure to local MongoDB: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error connecting to local MongoDB: {e}")
            return None

    def _ensure_indexes(self, db) -> bool:
        """
        Create necessary indexes in local database.

        Args:
            db: Local MongoDB database instance

        Returns:
            True if successful, False otherwise
        """
        try:
            collection = db[self.config.sync_db.local_mongo.collection]

            # ★ TRACEABILITY INDEXES: Support for unified trace system
            indexes = [
                # PRIMARY TRACE INDEX: registrationNumber (UNIQUE)
                ([("registrationNumber", 1)], {"unique": True}, "registration_number_idx"),

                # Component trace indexes
                ([("trace.docreciv.unit_id", 1)], "trace_docreciv_unit_idx"),
                ([("unit_id", 1)], "unit_idx"),  # Legacy support

                # Business keys (NOT unique - one purchase can have multiple protocols)
                ([("purchaseInfo.purchaseNoticeNumber", 1)], "purchase_notice_idx"),
                ([("purchaseNoticeNumber", 1), ("source", 1)], "pn_source_idx"),  # Legacy

                # Status and date indexes
                ([("status", 1)], "status_idx"),
                ([("loadDate", -1)], "loaddate_idx"),  # Descending for recent-first queries
                ([("created_at", -1)], "created_at_idx"),

                # History indexing for temporal queries
                ([("history.timestamp", -1)], "history_timestamp_idx"),
            ]

            for idx_def in indexes:
                if len(idx_def) == 3:
                    index_spec, index_options, index_name = idx_def
                else:
                    index_spec, index_name = idx_def
                    index_options = {}

                try:
                    collection.create_index(index_spec, name=index_name, **index_options)
                    self.logger.debug(f"Created index: {index_name}")
                except Exception as e:
                    self.logger.warning(f"Could not create index {index_name}: {e}")

            self.logger.info("Indexes ensured in local MongoDB (including traceability indexes)")
            return True

        except Exception as e:
            self.logger.error(f"Error ensuring indexes: {e}")
            return False

    def _generate_unit_id(self) -> str:
        """
        Generate unique unit ID in format UNIT_<16hex>.
        
        Returns:
            Unique unit ID string
        """
        return f"UNIT_{uuid.uuid4().hex[:16]}"

    def _extract_urls_from_attachments(self, raw_doc: Dict[str, Any]) -> Tuple[List[Dict], Optional[datetime]]:
        """
        Extract URLs from attachments field with support for different structures.
        
        Args:
            raw_doc: Raw protocol document from MongoDB
            
        Returns:
            Tuple of (list of URL dictionaries, load date)
        """
        urls = []
        load_date = raw_doc.get("loadDate")

        def add_from_doc(doc: Dict[str, Any]) -> None:
            """Add URL information from document attachment."""
            # Track attachment types for statistics
            attachment_type = "unknown"
            if "url" in doc:
                attachment_type = "url"
            elif "downloadUrl" in doc:
                attachment_type = "downloadUrl"
            elif "fileUrl" in doc:
                attachment_type = "fileUrl"
            
            self.counters["attachment_types"][attachment_type] = \
                self.counters["attachment_types"].get(attachment_type, 0) + 1
            
            # Extract URL
            url = doc.get("url") or doc.get("downloadUrl") or doc.get("fileUrl")
            if url:
                url_info = {
                    "url": url,
                    "fileName": doc.get("fileName") or doc.get("name") or "",
                    "guid": doc.get("guid"),
                    "contentUid": doc.get("contentUid"),
                    "description": doc.get("description"),
                }
                urls.append(url_info)

        # Handle different attachment structures
        attachments = raw_doc.get("attachments")

        if isinstance(attachments, dict):
            # Handle document array structure
            docs_field = attachments.get("document", [])
            if isinstance(docs_field, dict):
                docs_field = [docs_field]
            if isinstance(docs_field, list):
                for item in docs_field:
                    if isinstance(item, dict):
                        add_from_doc(item)
        elif isinstance(attachments, list):
            # Handle direct array structure
            for item in attachments:
                if isinstance(item, dict):
                    add_from_doc(item)

        # Update counters for URL statistics
        if len(urls) == 0:
            self.counters["no_url"] += 1
        elif len(urls) == 1:
            self.counters["single_url"] += 1
        else:
            self.counters["multi_url"] += 1

        return urls, load_date

    def _create_protocol_document(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create protocol document for insertion into local MongoDB.

        Args:
            raw_doc: Raw protocol document from remote MongoDB

        Returns:
            Document ready for insertion
        """
        # Extract purchase notice number (metadata)
        purchase_info = raw_doc.get("purchaseInfo", {})
        pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None

        # Extract URLs from attachments
        urls, load_date = self._extract_urls_from_attachments(raw_doc)

        # ★ PRIMARY TRACE ID: registrationNumber from root document
        reg_number = raw_doc.get("registrationNumber", "")

        # Get current timestamp
        now_ts = datetime.utcnow()

        # Generate unit_id (local file system identifier)
        unit_id = self._generate_unit_id()

        # Create document with service fields
        doc_to_insert = {
            # FULL PROTOCOL DATA FROM MONGODB (excluding _id)
            **{k: v for k, v in raw_doc.items() if k != '_id'},

            # ★ TRACEABILITY FIELDS
            "registrationNumber": reg_number,  # PRIMARY TRACE ID from purchaseProtocol collection

            # Service fields for preprocessing
            "unit_id": unit_id,
            "urls": urls,
            "multi_url": len(urls) > 1,
            "url_count": len(urls),
            "source": "remote_mongo_direct",
            "status": "pending",
            "created_at": now_ts,
            "updated_at": now_ts,

            # ★ TRACE STRUCTURE: Component-level tracking
            "trace": {
                "docreciv": {
                    "unit_id": unit_id,
                    "synced_at": now_ts.isoformat() + "Z",
                    "registrationNumber": reg_number,
                },
                # Future components will add their trace data here:
                # "docprep": {...},
                # "docling": {...},
                # "llm_qaenrich": {...},
            },

            # ★ HISTORY: Chronological processing log
            "history": [
                {
                    "component": "docreciv",
                    "action": "synced",
                    "timestamp": now_ts.isoformat() + "Z",
                    "registrationNumber": reg_number,
                }
            ],
        }

        return doc_to_insert

    def _collect_statistics(self) -> Dict[str, Any]:
        """
        Collect detailed statistics from counters.
        
        Returns:
            Dictionary with statistics
        """
        stats = {
            "url_distribution": {
                "single_url": self.counters["single_url"],
                "multi_url": self.counters["multi_url"],
                "no_url": self.counters["no_url"]
            },
            "attachment_types": self.counters["attachment_types"],
            "processing_times": self.counters["processing_times"],
            "error_types": self.counters["error_types"]
        }
        
        # Calculate average processing time
        if self.counters["processing_times"]:
            stats["average_processing_time"] = sum(self.counters["processing_times"]) / len(self.counters["processing_times"])
            stats["max_processing_time"] = max(self.counters["processing_times"])
            stats["min_processing_time"] = min(self.counters["processing_times"])
        
        return stats

    def sync_protocols_for_date(
        self,
        target_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> EnhancedSyncResult:
        """
        Perform full synchronization of protocols for a specific date.
        
        Args:
            target_date: Target date for synchronization (defaults to yesterday)
            limit: Maximum number of documents to process
            
        Returns:
            EnhancedSyncResult with detailed metrics
        """
        start_time = time.time()
        self.reset_counters()
        
        if target_date is None:
            target_date = datetime.utcnow() - timedelta(days=1)
            
        if limit is None:
            limit = 200

        self.logger.info("Starting protocol synchronization")
        self.logger.info(f"Target date: {target_date.date()}")
        self.logger.info(f"Limit: {limit}")

        # Stage 1: Check local MongoDB connection
        self.logger.info("Stage 1: Checking local MongoDB connection...")
        self.local_client = self._get_local_mongo_client()
        if not self.local_client:
            return EnhancedSyncResult(
                status="error",
                message="Failed to connect to local MongoDB",
                date=target_date.date().isoformat(),
                errors=["Local MongoDB connection failed"]
            )

        # Stage 2: Check remote MongoDB connection
        self.logger.info("Stage 2: Checking remote MongoDB connection...")
        self.remote_client = self._get_remote_mongo_client()
        if not self.remote_client:
            self.local_client.close()
            return EnhancedSyncResult(
                status="error",
                message="Failed to connect to remote MongoDB",
                date=target_date.date().isoformat(),
                errors=["Remote MongoDB connection failed"]
            )

        try:
            # Get collections
            remote_db = self.remote_client[self.config.sync_db.remote_mongo.db]
            remote_collection = remote_db[self.config.sync_db.remote_mongo.collection]
            
            local_db = self.local_client[self.config.sync_db.local_mongo.db]
            local_collection = local_db[self.config.sync_db.local_mongo.collection]
            
            # Ensure indexes
            self._ensure_indexes(local_db)
            
            # Stage 3: Check for protocol updates
            self.logger.info("Stage 3: Checking for protocol updates...")
            start_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
            end_dt = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)

            query = {
                "loadDate": {
                    "$gte": start_dt,
                    "$lte": end_dt,
                }
            }
            
            # Get document count estimate
            try:
                total_count = remote_collection.count_documents(query)
                self.logger.info(f"Found approximately {total_count} documents")
            except Exception as e:
                self.logger.warning(f"Could not get document count: {e}")
                total_count = 0
            
            # Stage 4: Synchronize database collections
            self.logger.info("Stage 4: Synchronizing database collections...")
            cursor = remote_collection.find(query, no_cursor_timeout=True, batch_size=self.config.sync_db.batch_size)
            
            # Apply limit if specified
            if limit > 0:
                cursor = cursor.limit(limit)
            
            # Process documents
            scanned = 0
            inserted = 0
            skipped_existing = 0
            errors_count = 0
            errors = []
            warnings = []
            
            batch = []
            
            for raw_doc in cursor:
                batch.append(raw_doc)
                scanned += 1
                
                # Process batch
                if len(batch) >= self.config.sync_db.batch_size:
                    self.logger.info(f"Processing batch of {len(batch)} documents...")
                    
                    for doc in batch:
                        try:
                            # Extract purchase notice number
                            purchase_info = doc.get("purchaseInfo", {})
                            pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
                            
                            if not pn:
                                errors_count += 1
                                error_msg = "Document missing purchaseNoticeNumber"
                                errors.append(error_msg)
                                self.counters["error_types"]["missing_pn"] = \
                                    self.counters["error_types"].get("missing_pn", 0) + 1
                                continue
                            
                            # Check for duplicates
                            existing = local_collection.find_one({
                                "purchaseNoticeNumber": str(pn),
                                "source": "remote_mongo_direct"
                            })
                            
                            if existing:
                                skipped_existing += 1
                                continue
                            
                            # Create document for insertion
                            doc_to_insert = self._create_protocol_document(doc)
                            
                            # Insert document
                            try:
                                local_collection.insert_one(doc_to_insert)
                                inserted += 1
                            except DuplicateKeyError:
                                # This is expected for some duplicates, just skip
                                skipped_existing += 1
                                continue
                            except Exception as e:
                                raise e
                            
                        except Exception as e:
                            errors_count += 1
                            error_msg = f"Error processing document: {str(e)}"
                            errors.append(error_msg)
                            self.counters["error_types"]["processing_error"] = \
                                self.counters["error_types"].get("processing_error", 0) + 1
                            self.logger.error(error_msg)
                    
                    batch = []
            
            # Process remaining documents
            if batch:
                self.logger.info(f"Processing final batch of {len(batch)} documents...")
                
                for doc in batch:
                    try:
                        # Extract purchase notice number
                        purchase_info = doc.get("purchaseInfo", {})
                        pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
                        
                        if not pn:
                            errors_count += 1
                            error_msg = "Document missing purchaseNoticeNumber"
                            errors.append(error_msg)
                            self.counters["error_types"]["missing_pn"] = \
                                self.counters["error_types"].get("missing_pn", 0) + 1
                            continue
                        
                        # Check for duplicates
                        existing = local_collection.find_one({
                            "purchaseNoticeNumber": str(pn),
                            "source": "remote_mongo_direct"
                        })
                        
                        if existing:
                            skipped_existing += 1
                            continue
                        
                        # Create document for insertion
                        doc_to_insert = self._create_protocol_document(doc)
                        
                        # Insert document
                        local_collection.insert_one(doc_to_insert)
                        inserted += 1
                        
                    except Exception as e:
                        errors_count += 1
                        error_msg = f"Error processing document: {str(e)}"
                        errors.append(error_msg)
                        self.counters["error_types"]["processing_error"] = \
                            self.counters["error_types"].get("processing_error", 0) + 1
                        self.logger.error(error_msg)
            
            # Stage 5: Generate report with metrics
            duration = time.time() - start_time
            self.logger.info("Stage 5: Generating metrics report...")
            
            # Collect detailed statistics
            statistics = self._collect_statistics()
            
            result = EnhancedSyncResult(
                status="success" if errors_count == 0 else "partial",
                message="Synchronization completed",
                date=target_date.date().isoformat(),
                scanned=scanned,
                inserted=inserted,
                skipped_existing=skipped_existing,
                errors_count=errors_count,
                duration=duration,
                errors=errors,
                warnings=warnings,
                statistics=statistics
            )
            
            # Log summary
            self.logger.info("Synchronization completed:")
            self.logger.info(f"  Scanned: {scanned}")
            self.logger.info(f"  Inserted: {inserted}")
            self.logger.info(f"  Skipped (duplicates): {skipped_existing}")
            self.logger.info(f"  Errors: {errors_count}")
            self.logger.info(f"  Duration: {duration:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_msg = f"Critical synchronization error: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return EnhancedSyncResult(
                status="error",
                message=error_msg,
                date=target_date.date().isoformat(),
                errors=[error_msg]
            )
            
        finally:
            # Close connections
            if self.remote_client:
                self.remote_client.close()
                self.logger.debug("Closed remote MongoDB connection")
            if self.local_client:
                self.local_client.close()
                self.logger.debug("Closed local MongoDB connection")

    def sync_protocols_for_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None
    ) -> EnhancedSyncResult:
        """
        Synchronize protocols for a date range.
        
        Args:
            start_date: Start date for synchronization
            end_date: End date for synchronization
            limit: Maximum number of documents to process
            
        Returns:
            EnhancedSyncResult with detailed metrics
        """
        start_time = time.time()
        self.reset_counters()
        
        if limit is None:
            limit = 0  # No limit

        self.logger.info("Starting protocol synchronization for date range")
        self.logger.info(f"Period: {start_date.date()} - {end_date.date()}")
        if limit > 0:
            self.logger.info(f"Limit: {limit}")

        # Stage 1: Check local MongoDB connection
        self.logger.info("Stage 1: Checking local MongoDB connection...")
        self.local_client = self._get_local_mongo_client()
        if not self.local_client:
            return EnhancedSyncResult(
                status="error",
                message="Failed to connect to local MongoDB",
                date=f"{start_date.date().isoformat()} - {end_date.date().isoformat()}",
                errors=["Local MongoDB connection failed"]
            )

        # Stage 2: Check remote MongoDB connection
        self.logger.info("Stage 2: Checking remote MongoDB connection...")
        self.remote_client = self._get_remote_mongo_client()
        if not self.remote_client:
            self.local_client.close()
            return EnhancedSyncResult(
                status="error",
                message="Failed to connect to remote MongoDB",
                date=f"{start_date.date().isoformat()} - {end_date.date().isoformat()}",
                errors=["Remote MongoDB connection failed"]
            )

        try:
            # Get collections
            remote_db = self.remote_client[self.config.sync_db.remote_mongo.db]
            remote_collection = remote_db[self.config.sync_db.remote_mongo.collection]
            
            local_db = self.local_client[self.config.sync_db.local_mongo.db]
            local_collection = local_db[self.config.sync_db.local_mongo.collection]
            
            # Ensure indexes
            self._ensure_indexes(local_db)
            
            # Stage 3: Check for protocol updates
            self.logger.info("Stage 3: Checking for protocol updates...")
            query = {
                "loadDate": {
                    "$gte": start_date,
                    "$lte": end_date,
                }
            }
            
            # Get document count estimate
            try:
                total_count = remote_collection.count_documents(query)
                self.logger.info(f"Found approximately {total_count} documents")
            except Exception as e:
                self.logger.warning(f"Could not get document count: {e}")
                total_count = 0
            
            # Stage 4: Synchronize database collections
            self.logger.info("Stage 4: Synchronizing database collections...")
            cursor = remote_collection.find(query, no_cursor_timeout=True, batch_size=self.config.sync_db.batch_size)
            
            # Apply limit if specified
            if limit > 0:
                cursor = cursor.limit(limit)
            
            # Process documents
            scanned = 0
            inserted = 0
            skipped_existing = 0
            errors_count = 0
            errors = []
            warnings = []
            
            batch = []
            
            for raw_doc in cursor:
                batch.append(raw_doc)
                scanned += 1
                
                # Process batch
                if len(batch) >= self.config.sync_db.batch_size:
                    self.logger.info(f"Processing batch of {len(batch)} documents...")
                    
                    for doc in batch:
                        try:
                            # Extract purchase notice number
                            purchase_info = doc.get("purchaseInfo", {})
                            pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
                            
                            if not pn:
                                errors_count += 1
                                error_msg = "Document missing purchaseNoticeNumber"
                                errors.append(error_msg)
                                self.counters["error_types"]["missing_pn"] = \
                                    self.counters["error_types"].get("missing_pn", 0) + 1
                                continue
                            
                            # Check for duplicates
                            existing = local_collection.find_one({
                                "purchaseNoticeNumber": str(pn),
                                "source": "remote_mongo_direct"
                            })
                            
                            if existing:
                                skipped_existing += 1
                                continue
                            
                            # Create document for insertion
                            doc_to_insert = self._create_protocol_document(doc)
                            
                            # Insert document
                            try:
                                local_collection.insert_one(doc_to_insert)
                                inserted += 1
                            except DuplicateKeyError:
                                # This is expected for some duplicates, just skip
                                skipped_existing += 1
                                continue
                            except Exception as e:
                                raise e
                            
                        except Exception as e:
                            errors_count += 1
                            error_msg = f"Error processing document: {str(e)}"
                            errors.append(error_msg)
                            self.counters["error_types"]["processing_error"] = \
                                self.counters["error_types"].get("processing_error", 0) + 1
                            self.logger.error(error_msg)
                    
                    batch = []
            
            # Process remaining documents
            if batch:
                self.logger.info(f"Processing final batch of {len(batch)} documents...")
                
                for doc in batch:
                    try:
                        # Extract purchase notice number
                        purchase_info = doc.get("purchaseInfo", {})
                        pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
                        
                        if not pn:
                            errors_count += 1
                            error_msg = "Document missing purchaseNoticeNumber"
                            errors.append(error_msg)
                            self.counters["error_types"]["missing_pn"] = \
                                self.counters["error_types"].get("missing_pn", 0) + 1
                            continue
                        
                        # Check for duplicates
                        existing = local_collection.find_one({
                            "purchaseNoticeNumber": str(pn),
                            "source": "remote_mongo_direct"
                        })
                        
                        if existing:
                            skipped_existing += 1
                            continue
                        
                        # Create document for insertion
                        doc_to_insert = self._create_protocol_document(doc)
                        
                        # Insert document
                        local_collection.insert_one(doc_to_insert)
                        inserted += 1
                        
                    except Exception as e:
                        errors_count += 1
                        error_msg = f"Error processing document: {str(e)}"
                        errors.append(error_msg)
                        self.counters["error_types"]["processing_error"] = \
                            self.counters["error_types"].get("processing_error", 0) + 1
                        self.logger.error(error_msg)
            
            # Stage 5: Generate report with metrics
            duration = time.time() - start_time
            self.logger.info("Stage 5: Generating metrics report...")
            
            # Collect detailed statistics
            statistics = self._collect_statistics()
            
            result = EnhancedSyncResult(
                status="success" if errors_count == 0 else "partial",
                message="Synchronization completed",
                date=f"{start_date.date().isoformat()} - {end_date.date().isoformat()}",
                scanned=scanned,
                inserted=inserted,
                skipped_existing=skipped_existing,
                errors_count=errors_count,
                duration=duration,
                errors=errors,
                warnings=warnings,
                statistics=statistics
            )
            
            # Log summary
            self.logger.info("Synchronization completed:")
            self.logger.info(f"  Scanned: {scanned}")
            self.logger.info(f"  Inserted: {inserted}")
            self.logger.info(f"  Skipped (duplicates): {skipped_existing}")
            self.logger.info(f"  Errors: {errors_count}")
            self.logger.info(f"  Duration: {duration:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_msg = f"Critical synchronization error: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return EnhancedSyncResult(
                status="error",
                message=error_msg,
                date=f"{start_date.date().isoformat()} - {end_date.date().isoformat()}",
                errors=[error_msg]
            )
            
        finally:
            # Close connections
            if self.remote_client:
                self.remote_client.close()
                self.logger.debug("Closed remote MongoDB connection")
            if self.local_client:
                self.local_client.close()
                self.logger.debug("Closed local MongoDB connection")

    def sync_daily_updates(self, limit: Optional[int] = None) -> EnhancedSyncResult:
        """
        Daily update - synchronize protocols for yesterday.
        
        Args:
            limit: Maximum number of documents to process
            
        Returns:
            EnhancedSyncResult with detailed metrics
        """
        self.logger.info("Starting daily protocol update")
        yesterday = datetime.utcnow() - timedelta(days=1)
        return self.sync_protocols_for_date(yesterday, limit)

    def sync_full_collection(self, days: int = 14) -> EnhancedSyncResult:
        """
        Full collection synchronization for specified number of days.
        
        Args:
            days: Number of days to synchronize
            
        Returns:
            EnhancedSyncResult with detailed metrics
        """
        self.logger.info(f"Starting full synchronization for {days} days")
        
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        
        return self.sync_protocols_for_date_range(start_date, end_date)

    def close(self) -> None:
        """Close all database connections."""
        if self.remote_client:
            self.remote_client.close()
            self.logger.debug("Closed remote MongoDB connection")
        if self.local_client:
            self.local_client.close()
            self.logger.debug("Closed local MongoDB connection")


def main():
    """Entry point for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Sync Service")
    parser.add_argument(
        "command",
        choices=["sync-date", "sync-range", "sync-full", "sync-daily"],
        help="Command to execute"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Target date for sync-date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for sync-range (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for sync-range (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Number of days for sync-full (default: 14)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of documents (0 = no limit)"
    )
    
    args = parser.parse_args()
    
    # Create service
    service = EnhancedSyncService()
    
    try:
        # Execute command
        if args.command == "sync-date":
            if not args.date:
                print("❌ --date argument is required for sync-date")
                return 1
            try:
                target_date = datetime.strptime(args.date, "%Y-%m-%d")
                result = service.sync_protocols_for_date(target_date, args.limit)
            except ValueError:
                print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD")
                return 1
        elif args.command == "sync-range":
            if not args.start_date or not args.end_date:
                print("❌ --start-date and --end-date arguments are required for sync-range")
                return 1
            try:
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
                result = service.sync_protocols_for_date_range(start_date, end_date, args.limit)
            except ValueError as e:
                print(f"❌ Invalid date format: {e}. Use YYYY-MM-DD")
                return 1
        elif args.command == "sync-full":
            result = service.sync_full_collection(args.days)
        elif args.command == "sync-daily":
            result = service.sync_daily_updates(args.limit)
        else:
            print(f"❌ Unknown command: {args.command}")
            return 1
        
        # Print result
        print("\n" + "="*60)
        print("📊 SYNCHRONIZATION RESULTS")
        print("="*60)
        
        if result.status == "success":
            print("✅ SUCCESS!")
        elif result.status == "partial":
            print("⚠️  PARTIAL SUCCESS (with errors)")
        else:
            print("❌ ERROR!")
        
        print(f"📅 Period: {result.date}")
        print(f"🔍 Scanned: {result.scanned}")
        print(f"💾 Inserted: {result.inserted}")
        print(f"⏭️  Skipped (duplicates): {result.skipped_existing}")
        print(f"❌ Errors: {result.errors_count}")
        print(f"⏱️  Duration: {result.duration:.2f} seconds")
        
        if result.errors:
            print("\n📝 Errors:")
            for i, error in enumerate(result.errors[:5], 1):
                print(f"   {i}. {error[:100]}{'...' if len(error) > 100 else ''}")
            if len(result.errors) > 5:
                print(f"   ... and {len(result.errors) - 5} more errors")
        
        if result.warnings:
            print("\n⚠️  Warnings:")
            for i, warning in enumerate(result.warnings[:3], 1):
                print(f"   {i}. {warning}")
        
        if result.statistics:
            print("\n📈 Statistics:")
            stats = result.statistics
            if "url_distribution" in stats:
                url_dist = stats["url_distribution"]
                print(f"   URL Distribution:")
                print(f"     Single URL: {url_dist['single_url']}")
                print(f"     Multi URL: {url_dist['multi_url']}")
                print(f"     No URL: {url_dist['no_url']}")
            
            if "attachment_types" in stats and stats["attachment_types"]:
                print(f"   Attachment Types:")
                for att_type, count in stats["attachment_types"].items():
                    print(f"     {att_type}: {count}")
            
            if "average_processing_time" in stats:
                print(f"   Average Processing Time: {stats['average_processing_time']:.4f}s")
        
        return 0 if result.status in ["success", "partial"] else 1
        
    finally:
        service.close()


if __name__ == "__main__":
    exit(main())