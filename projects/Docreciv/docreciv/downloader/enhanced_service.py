"""
Enhanced Protocol Downloader Service.

Provides improved downloading of procurement protocols with better error handling,
reporting, and monitoring capabilities.
"""

import os
import time
import json
import logging
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import ObjectId

from ..core.config import get_config
from ..vpn_utils import ensure_vpn_connected, check_zakupki_access, get_vpn_status, is_openvpn_running, is_vpn_interface_up, check_vpn_routes
from .utils import sanitize_filename, get_metadata_client, check_zakupki_health
from .meta_generator import MetaGenerator
from .file_manager import FileManager
from .models import DownloadRequest
from .status_tracker import DownloadStatusTracker

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler with formatting
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


@dataclass
class DownloadResult:
    """Enhanced result of download operation."""
    status: str
    message: str
    processed: int = 0
    downloaded: int = 0
    failed: int = 0
    duration: float = 0.0
    errors: Optional[List[str]] = None
    warnings: Optional[List[str]] = None
    statistics: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.statistics is None:
            self.statistics = {}


class EnhancedProtocolDownloader:
    """Enhanced service for downloading procurement protocols."""

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize the enhanced downloader.
        
        Args:
            output_dir: Directory for saving downloaded files
        """
        self.config = get_config().downloader
        self.output_dir = output_dir or self.config.output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize file manager and meta generator
        self.file_manager = FileManager(self.output_dir)
        self.meta_generator = MetaGenerator()
        self.status_tracker = DownloadStatusTracker(self.output_dir)
        
        # Initialize counters for statistics
        self.reset_counters()
        
        # Create session with retry mechanism
        self.session = self._create_session()
        
        logger.info(f"Initialized EnhancedProtocolDownloader with output dir: {self.output_dir}")

    def reset_counters(self):
        """Reset all internal counters."""
        self.counters = {
            "file_sizes": [],
            "download_times": [],
            "error_types": {},
            "file_types": {},
            "success_by_domain": {},
            "failed_by_domain": {}
        }

    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry mechanism and proper headers.
        
        Returns:
            Configured requests session
        """
        session = requests.Session()
        
        # Set up retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"]
        )
        
        # Mount adapter with retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers
        session.headers.update(self.config.browser_headers)
        
        return session

    def _update_statistics(self, url: str, file_size: int, download_time: float, success: bool):
        """
        Update internal statistics counters.
        
        Args:
            url: Downloaded URL
            file_size: Size of downloaded file in bytes
            download_time: Time taken to download in seconds
            success: Whether download was successful
        """
        # Track file sizes and download times
        self.counters["file_sizes"].append(file_size)
        self.counters["download_times"].append(download_time)
        
        # Track file types
        file_extension = Path(url).suffix.lower() if '.' in url else "unknown"
        self.counters["file_types"][file_extension] = \
            self.counters["file_types"].get(file_extension, 0) + 1
        
        # Track success/failure by domain
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
            if success:
                self.counters["success_by_domain"][domain] = \
                    self.counters["success_by_domain"].get(domain, 0) + 1
            else:
                self.counters["failed_by_domain"][domain] = \
                    self.counters["failed_by_domain"].get(domain, 0) + 1
        except Exception:
            pass  # Ignore parsing errors

    def _collect_statistics(self) -> Dict[str, Any]:
        """
        Collect detailed statistics from counters.
        
        Returns:
            Dictionary with statistics
        """
        stats = {}
        
        # File size statistics
        if self.counters["file_sizes"]:
            stats["file_sizes"] = {
                "total_bytes": sum(self.counters["file_sizes"]),
                "average_bytes": sum(self.counters["file_sizes"]) / len(self.counters["file_sizes"]),
                "max_bytes": max(self.counters["file_sizes"]),
                "min_bytes": min(self.counters["file_sizes"])
            }
        
        # Download time statistics
        if self.counters["download_times"]:
            stats["download_times"] = {
                "total_seconds": sum(self.counters["download_times"]),
                "average_seconds": sum(self.counters["download_times"]) / len(self.counters["download_times"]),
                "max_seconds": max(self.counters["download_times"]),
                "min_seconds": min(self.counters["download_times"])
            }
        
        # File type distribution
        stats["file_types"] = self.counters["file_types"]
        
        # Success/failure by domain
        stats["success_by_domain"] = self.counters["success_by_domain"]
        stats["failed_by_domain"] = self.counters["failed_by_domain"]
        
        # Error type distribution
        stats["error_types"] = self.counters["error_types"]
        
        return stats

    def _download_single_file(self, url: str, output_path: Path) -> bool:
        """
        Download a single file with enhanced error handling.
        
        Args:
            url: URL to download
            output_path: Path to save the file
            
        Returns:
            True if successful, False otherwise
        """
        start_time = time.time()
        file_size = 0
        
        try:
            logger.debug(f"Downloading {url} to {output_path}")
            
            # Create parent directories
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Make request
            response = self.session.get(
                url,
                timeout=self.config.download_http_timeout,
                stream=True
            )
            response.raise_for_status()
            
            # Save file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Get file size
            file_size = output_path.stat().st_size
            
            # Calculate download time
            download_time = time.time() - start_time
            
            # Update statistics
            self._update_statistics(url, file_size, download_time, True)
            
            logger.info(f"Successfully downloaded {url} ({file_size} bytes in {download_time:.2f}s)")
            return True
            
        except requests.exceptions.Timeout:
            error_msg = f"Timeout downloading {url}"
            logger.error(error_msg)
            self.counters["error_types"]["timeout"] = \
                self.counters["error_types"].get("timeout", 0) + 1
            self._update_statistics(url, file_size, time.time() - start_time, False)
            return False
            
        except requests.exceptions.ConnectionError:
            error_msg = f"Connection error downloading {url}"
            logger.error(error_msg)
            self.counters["error_types"]["connection"] = \
                self.counters["error_types"].get("connection", 0) + 1
            self._update_statistics(url, file_size, time.time() - start_time, False)
            return False
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error {e.response.status_code} downloading {url}"
            logger.error(error_msg)
            error_type = f"http_{e.response.status_code}"
            self.counters["error_types"][error_type] = \
                self.counters["error_types"].get(error_type, 0) + 1
            self._update_statistics(url, file_size, time.time() - start_time, False)
            return False
            
        except Exception as e:
            error_msg = f"Error downloading {url}: {str(e)}"
            logger.error(error_msg)
            self.counters["error_types"]["other"] = \
                self.counters["error_types"].get("other", 0) + 1
            self._update_statistics(url, file_size, time.time() - start_time, False)
            return False

    def _process_single_protocol(
        self,
        protocol: Dict[str, Any],
        collection,
        skip_existing: bool = True,
        force_reload: bool = False,
        max_urls_per_unit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Process a single protocol with enhanced error handling.
        
        Args:
            protocol: Protocol document from MongoDB
            collection: MongoDB collection for updates
            skip_existing: Пропускать существующие UNIT директории
            force_reload: Принудительная повторная загрузка
            max_urls_per_unit: Максимальное количество URL на UNIT (None = использовать config)
            
        Returns:
            Dictionary with processing results
        """
        unit_id = protocol.get("unit_id")
        urls = protocol.get("urls", [])
        
        if not unit_id:
            return {"downloaded": 0, "failed": 1, "error": "Protocol missing unit_id"}
        
        if not urls:
            try:
                collection.update_one(
                    {"unit_id": unit_id},
                    {"$set": {"status": "downloaded", "updated_at": datetime.utcnow()}}
                )
                return {"downloaded": 0, "failed": 0}
            except Exception as e:
                return {"downloaded": 0, "failed": 1, "error": f"Error updating status: {e}"}
        
        # Extract loadDate from protocol for directory structure
        load_date = protocol.get("loadDate")
        if isinstance(load_date, datetime):
            date_str = load_date.strftime("%Y-%m-%d")
        elif isinstance(load_date, str):
            # Try to parse string date
            try:
                date_obj = datetime.strptime(load_date, "%Y-%m-%d")
                date_str = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                # Fallback to current date if parsing fails
                date_str = datetime.utcnow().strftime("%Y-%m-%d")
        else:
            # Fallback to current date if loadDate is missing or invalid
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
        
        # Check if unit directory exists
        unit_dir = self.file_manager.get_unit_dir(date_str, unit_id)
        unit_exists = self.file_manager.unit_dir_exists(date_str, unit_id)
        
        # Handle skip_existing and force_reload
        if unit_exists and not force_reload:
            if skip_existing:
                logger.debug(f"Skipping existing unit directory: {unit_dir}")
                try:
                    collection.update_one(
                        {"unit_id": unit_id},
                        {"$set": {"status": "downloaded", "updated_at": datetime.utcnow()}}
                    )
                except Exception as e:
                    logger.error(f"Error updating status for skipped unit {unit_id}: {e}")
                return {"downloaded": 0, "failed": 0, "skipped": True}
            else:
                # Unit exists but skip_existing=False, so we continue
                logger.info(f"Unit directory exists but continuing: {unit_dir}")
        elif unit_exists and force_reload:
            logger.info(f"Force reload requested for existing unit: {unit_dir}")
            # При force_reload удаляем существующие файлы (кроме meta)
            try:
                for file_path in unit_dir.iterdir():
                    if file_path.is_file() and file_path.name not in ["unit.meta.json", "raw_url_map.json"]:
                        file_path.unlink()
                        logger.debug(f"Removed existing file: {file_path}")
            except Exception as e:
                logger.warning(f"Error removing existing files for force reload: {e}")
        
        # Create directory structure: INPUT_DIR/YYYY-MM-DD/Input/UNIT_xxx/
        # Use file manager to create unit directory (создаст если не существует)
        unit_dir = self.file_manager.create_unit_dir(date_str, unit_id)
        
        logger.debug(f"Created unit directory: {unit_dir} for protocol {unit_id} (date: {date_str})")
        
        downloaded_count = 0
        failed_count = 0
        
        # Limit number of URLs to download
        max_urls = max_urls_per_unit if max_urls_per_unit is not None else self.config.max_urls_per_protocol
        urls_to_download = urls[:max_urls]
        
        # Track download results for meta files
        download_results = []  # List of dicts with url, filename, status
        
        # Download files concurrently
        with ThreadPoolExecutor(max_workers=self.config.download_concurrency) as executor:
            futures = []
            
            for i, url_info in enumerate(urls_to_download):
                url = url_info.get("url")
                if not url:
                    continue
                
                # Create filename
                original_name = url_info.get("fileName") or f"document_{i+1}.pdf"
                safe_name = sanitize_filename(original_name)
                file_path = unit_dir / safe_name
                
                future = executor.submit(self._download_single_file, url, file_path)
                futures.append((future, file_path, url, original_name, url_info))
            
            # Collect results
            for future, file_path, url, original_name, url_info in futures:
                try:
                    success = future.result()
                    download_results.append({
                        "url": url,
                        "filename": file_path.name,
                        "original_filename": original_name,
                        "status": "ok" if success else "failed",
                        "guid": url_info.get("guid"),
                        "contentUid": url_info.get("contentUid"),
                        "description": url_info.get("description")
                    })
                    if success:
                        downloaded_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    download_results.append({
                        "url": url,
                        "filename": file_path.name if 'file_path' in locals() else "unknown",
                        "original_filename": original_name,
                        "status": "error",
                        "error": str(e)
                    })
                    logger.error(f"Error waiting for download result for {url}: {e}")
        
        # Create meta files
        try:
            self.meta_generator.create_unit_meta(
                unit_dir, unit_id, protocol, date_str,
                downloaded_count, failed_count, len(urls_to_download)
            )
            self.meta_generator.create_raw_url_map(unit_dir, download_results)
        except Exception as e:
            logger.error(f"Error creating meta files for {unit_id}: {e}")
            # Don't fail the whole operation if meta files fail
        
        # Update protocol status in MongoDB
        try:
            collection.update_one(
                {"unit_id": unit_id},
                {"$set": {"status": "downloaded", "updated_at": datetime.utcnow()}}
            )
        except Exception as e:
            logger.error(f"Error updating status for {unit_id}: {e}")
            failed_count += 1
        
        return {
            "downloaded": downloaded_count,
            "failed": failed_count,
            "error": None if failed_count == 0 else f"Failed to download {failed_count} documents"
        }
    

    def process_pending_protocols(self, limit: int = 0, target_date: Optional[datetime] = None) -> DownloadResult:
        """
        Process pending protocols with enhanced error handling and reporting.
        
        Args:
            limit: Maximum number of protocols to process (0 = no limit)
            target_date: Optional date to filter protocols by loadDate
            
        Returns:
            DownloadResult with detailed metrics
        """
        start_time = time.time()
        self.reset_counters()
        
        logger.info(f"Starting protocol processing with limit: {limit or 'no limit'}")
        
        # Check VPN connection before downloading
        logger.info("Checking VPN connection for zakupki.gov.ru access...")
        
        # Получаем детальный статус VPN
        vpn_status = get_vpn_status()
        
        # Логируем детальную информацию о VPN
        logger.info(f"VPN Status: OpenVPN running={vpn_status['openvpn_running']}, "
                   f"Interface up={vpn_status['interface_up']}, "
                   f"Overall status={vpn_status['overall_status']}")
        
        # Проверяем OpenVPN процесс
        if not vpn_status["openvpn_running"]:
            error_msg = "OpenVPN process is not running. Please start OpenVPN first."
            logger.error(error_msg)
            vpn_config = os.environ.get('VPN_CONFIG_FILE', '/root/winners_preprocessor/final_preprocessing/receiver/vitaly_bychkov.ovpn')
            logger.error(f"Example: sudo openvpn --config {vpn_config}")
            return DownloadResult(
                status="error",
                message=error_msg,
                duration=0.0,
                errors=[error_msg]
            )
        
        # Проверяем VPN интерфейс
        if not vpn_status["interface_up"]:
            error_msg = "VPN interface (tun0/tap0) is not up. VPN tunnel may not be established."
            logger.error(error_msg)
            return DownloadResult(
                status="error",
                message=error_msg,
                duration=0.0,
                errors=[error_msg]
            )
        
        # Проверяем маршруты к zakupki.gov.ru
        routes = check_vpn_routes(["zakupki.gov.ru", "www.zakupki.gov.ru"])
        if not any(routes.values()):
            logger.warning("Route to zakupki.gov.ru not found through VPN interface.")
            logger.warning("This may indicate that route-up-zakupki.sh was not executed or VPN routes are not configured.")
        
        # Проверяем доступность через ensure_vpn_connected для дополнительной проверки
        vpn_connected, vpn_message = ensure_vpn_connected()
        if not vpn_connected:
            logger.error(f"VPN check failed: {vpn_message}")
            return DownloadResult(
                status="error",
                message=f"VPN connection required: {vpn_message}",
                duration=0.0,
                errors=[f"VPN not connected: {vpn_message}"]
            )
        
        # Check zakupki.gov.ru availability
        logger.info("Checking zakupki.gov.ru availability...")
        zakupki_accessible, zakupki_url, response_time = check_zakupki_access()
        if not zakupki_accessible:
            return DownloadResult(
                status="error",
                message=f"zakupki.gov.ru unavailable: {zakupki_url}",
                duration=0.0,
                errors=[f"zakupki.gov.ru unavailable: {zakupki_url}"]
            )
        
        logger.info(f"zakupki.gov.ru is available (response time: {response_time:.2f}s)")
        
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = get_metadata_client()
        if not client:
            return DownloadResult(
                status="error",
                message="Failed to connect to MongoDB",
                duration=0.0,
                errors=["MongoDB connection failed"]
            )
        
        try:
            # Get database and collection
            db = client[self.config.mongo.db]
            collection = db[self.config.mongo.collection]
            logger.info("Connected to MongoDB")
            
            # Find pending protocols
            logger.info("Finding pending protocols...")
            query = {"status": "pending", "source": "remote_mongo_direct"}
            
            # Add date filter if specified
            if target_date:
                # Filter by loadDate (can be datetime or string)
                date_start = datetime.combine(target_date.date(), datetime.min.time())
                date_end = datetime.combine(target_date.date(), datetime.max.time())
                query["loadDate"] = {"$gte": date_start, "$lte": date_end}
                logger.info(f"Filtering by date: {target_date.strftime('%Y-%m-%d')}")
            
            cursor = collection.find(query).sort("loadDate", 1)  # Sort by date ascending
            
            if limit and limit > 0:
                cursor = cursor.limit(limit)
            
            protocols = list(cursor)
            logger.info(f"Found {len(protocols)} pending protocols")
            
            if not protocols:
                return DownloadResult(
                    status="success",
                    message="No pending protocols to process",
                    duration=0.0
                )
            
            # Process protocols concurrently
            logger.info(f"Processing {len(protocols)} protocols...")
            processed = 0
            downloaded = 0
            failed = 0
            errors = []
            warnings = []
            
            with ThreadPoolExecutor(max_workers=self.config.protocols_concurrency) as executor:
                futures = []
                
                for protocol in protocols:
                    if limit and limit > 0 and processed >= limit:
                        break
                    
                    # Use default parameters for backward compatibility
                    future = executor.submit(
                        self._process_single_protocol,
                        protocol,
                        collection,
                        skip_existing=True,  # Default behavior
                        force_reload=False,  # Default behavior
                        max_urls_per_unit=None  # Use config default
                    )
                    futures.append(future)
                    processed += 1
                
                # Collect results
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        downloaded += result.get("downloaded", 0)
                        failed += result.get("failed", 0)
                        if result.get("error"):
                            errors.append(result["error"])
                    except Exception as e:
                        failed += 1
                        error_msg = f"Error processing protocol: {str(e)}"
                        errors.append(error_msg)
                        self.counters["error_types"]["protocol_processing"] = \
                            self.counters["error_types"].get("protocol_processing", 0) + 1
                        logger.error(error_msg)
            
            # Collect final statistics
            duration = time.time() - start_time
            statistics = self._collect_statistics()
            
            result = DownloadResult(
                status="success" if failed == 0 else "partial",
                message="Protocol processing completed",
                processed=processed,
                downloaded=downloaded,
                failed=failed,
                duration=duration,
                errors=errors,
                warnings=warnings,
                statistics=statistics
            )
            
            # Log summary
            logger.info("Protocol processing completed:")
            logger.info(f"  Processed: {processed}")
            logger.info(f"  Downloaded: {downloaded}")
            logger.info(f"  Failed: {failed}")
            logger.info(f"  Duration: {duration:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_msg = f"Critical error during protocol processing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return DownloadResult(
                status="error",
                message=error_msg,
                duration=time.time() - start_time,
                errors=[error_msg]
            )
            
        finally:
            if client:
                client.close()
                logger.debug("Closed MongoDB connection")

    def process_download_request(self, request: DownloadRequest) -> DownloadResult:
        """
        Process download request with enhanced features.
        
        Args:
            request: DownloadRequest with all parameters
            
        Returns:
            DownloadResult with detailed metrics
        """
        start_time = time.time()
        self.reset_counters()
        
        logger.info(f"Starting download request: max_units={request.max_units_per_run}, "
                   f"dry_run={request.dry_run}, force_reload={request.force_reload}")
        
        # Check VPN connection before downloading (skip in dry-run)
        if not request.dry_run:
            logger.info("Checking VPN connection for zakupki.gov.ru access...")
            vpn_status = get_vpn_status()
            
            if not vpn_status["openvpn_running"]:
                error_msg = "OpenVPN process is not running. Please start OpenVPN first."
                logger.error(error_msg)
                return DownloadResult(
                    status="error",
                    message=error_msg,
                    duration=0.0,
                    errors=[error_msg]
                )
            
            if not vpn_status["interface_up"]:
                error_msg = "VPN interface (tun0/tap0) is not up. VPN tunnel may not be established."
                logger.error(error_msg)
                return DownloadResult(
                    status="error",
                    message=error_msg,
                    duration=0.0,
                    errors=[error_msg]
                )
            
            vpn_connected, vpn_message = ensure_vpn_connected()
            if not vpn_connected:
                return DownloadResult(
                    status="error",
                    message=f"VPN connection required: {vpn_message}",
                    duration=0.0,
                    errors=[f"VPN not connected: {vpn_message}"]
                )
            
            zakupki_accessible, zakupki_url, response_time = check_zakupki_access()
            if not zakupki_accessible:
                return DownloadResult(
                    status="error",
                    message=f"zakupki.gov.ru unavailable: {zakupki_url}",
                    duration=0.0,
                    errors=[f"zakupki.gov.ru unavailable: {zakupki_url}"]
                )
            logger.info(f"zakupki.gov.ru is available (response time: {response_time:.2f}s)")
        
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = get_metadata_client()
        if not client:
            return DownloadResult(
                status="error",
                message="Failed to connect to MongoDB",
                duration=0.0,
                errors=["MongoDB connection failed"]
            )
        
        try:
            # Get database and collection
            db = client[self.config.mongo.db]
            collection = db[self.config.mongo.collection]
            logger.info("Connected to MongoDB")
            
            # Build query
            query = {"source": "remote_mongo_direct"}
            
            # Filter by record_ids if specified
            if request.record_ids:
                query["_id"] = {"$in": [ObjectId(rid) if len(rid) == 24 else rid for rid in request.record_ids]}
                logger.info(f"Filtering by record_ids: {len(request.record_ids)} records")
            else:
                # Filter by status (pending or downloaded if force_reload)
                if request.force_reload:
                    query["status"] = {"$in": ["pending", "downloaded"]}
                else:
                    query["status"] = "pending"
            
            # Add date range filter if specified
            # Используем loadDate для фильтрации (дата загрузки в MongoDB)
            if request.from_date and request.to_date:
                date_start = datetime.combine(request.from_date.date(), datetime.min.time())
                date_end = datetime.combine(request.to_date.date(), datetime.max.time())
                query["loadDate"] = {"$gte": date_start, "$lte": date_end}
                logger.info(f"Filtering by loadDate range: {request.from_date.date()} - {request.to_date.date()}")
            elif request.from_date:
                date_start = datetime.combine(request.from_date.date(), datetime.min.time())
                query["loadDate"] = {"$gte": date_start}
                logger.info(f"Filtering by loadDate from: {request.from_date.date()}")
            elif request.to_date:
                date_end = datetime.combine(request.to_date.date(), datetime.max.time())
                query["loadDate"] = {"$lte": date_end}
                logger.info(f"Filtering by loadDate to: {request.to_date.date()}")
            
            # Find protocols from database
            cursor = collection.find(query).sort("loadDate", 1)
            all_protocols = list(cursor)
            logger.info(f"Found {len(all_protocols)} protocols in database matching query")
            
            # Логируем детали запроса для отладки
            logger.info(f"Query details: status={query.get('status')}, "
                       f"from_date={request.from_date}, to_date={request.to_date}, "
                       f"force_reload={request.force_reload}, skip_existing={request.skip_existing}")
            
            # Filter out already downloaded units if skip_existing and not force_reload
            protocols = []
            if request.skip_existing and not request.force_reload:
                # Используем статус-трекер для проверки загруженных UNIT
                date_range = None
                if request.from_date and request.to_date:
                    date_range = (request.from_date, request.to_date)
                elif request.from_date:
                    date_range = (request.from_date, datetime.utcnow())
                elif request.to_date:
                    # Если только to_date, берем последние 365 дней
                    date_range = (request.to_date - timedelta(days=365), request.to_date)
                
                if date_range:
                    try:
                        # Сканируем файловую систему для актуального индекса
                        logger.info(f"Scanning file system for downloaded units in date range: {date_range[0].date()} - {date_range[1].date()}")
                        self.status_tracker.scan_downloaded_units(date_range=date_range, force_rescan=True)
                        
                        downloaded_record_ids = self.status_tracker.get_downloaded_record_ids(date_range)
                        logger.info(f"Found {len(downloaded_record_ids)} already downloaded units in file system")
                    except Exception as e:
                        logger.warning(f"Error scanning file system for downloaded units: {e}. Continuing without filtering.")
                        # Продолжаем без фильтрации, если сканирование не удалось
                        downloaded_record_ids = set()
                
                # Фильтруем протоколы
                filtered_count = 0
                for protocol in all_protocols:
                    record_id = str(protocol.get("_id", ""))
                    unit_id = protocol.get("unit_id")
                    
                    # Проверяем, загружен ли UNIT
                    if record_id in downloaded_record_ids:
                        # Проверяем более детально
                        load_date = protocol.get("loadDate")
                        date_str = None
                        if load_date:
                            if isinstance(load_date, datetime):
                                date_str = load_date.strftime("%Y-%m-%d")
                            elif isinstance(load_date, str):
                                try:
                                    date_obj = datetime.fromisoformat(str(load_date).replace('Z', '+00:00'))
                                    date_str = date_obj.strftime("%Y-%m-%d")
                                except (ValueError, TypeError):
                                    pass
                        
                        try:
                            if self.status_tracker.is_unit_downloaded(record_id=record_id, unit_id=unit_id, date=date_str):
                                filtered_count += 1
                                logger.debug(f"Skipping already downloaded unit: {unit_id} (record_id: {record_id})")
                                continue
                        except Exception as e:
                            logger.warning(f"Error checking unit download status for {unit_id}: {e}. Including in processing.")
                            # Включаем протокол в обработку, если проверка не удалась
                    
                    protocols.append(protocol)
                
                logger.info(f"After filtering downloaded units: {len(protocols)} protocols to process (filtered out {filtered_count} already downloaded)")
            else:
                # Не фильтруем, используем все протоколы
                protocols = all_protocols
            
            # Apply max_units_per_run limit
            if request.max_units_per_run > 0 and len(protocols) > request.max_units_per_run:
                protocols = protocols[:request.max_units_per_run]
                logger.info(f"Limited to {request.max_units_per_run} protocols")
            
            logger.info(f"Final protocols to process: {len(protocols)}")
            
            if not protocols:
                # Проверяем, есть ли вообще протоколы в БД для диагностики
                total_count = collection.count_documents({"source": "remote_mongo_direct"})
                pending_count = collection.count_documents({"source": "remote_mongo_direct", "status": "pending"})
                downloaded_count = collection.count_documents({"source": "remote_mongo_direct", "status": "downloaded"})
                
                logger.info(f"Database statistics: total={total_count}, pending={pending_count}, downloaded={downloaded_count}")
                
                # Формируем информативное сообщение
                message_parts = []
                if request.from_date and request.to_date:
                    message_parts.append(f"в диапазоне {request.from_date.date()} - {request.to_date.date()}")
                elif request.from_date:
                    message_parts.append(f"с {request.from_date.date()}")
                elif request.to_date:
                    message_parts.append(f"до {request.to_date.date()}")
                
                status_filter = "pending" if not request.force_reload else "pending или downloaded"
                message = f"Не найдено протоколов со статусом '{status_filter}'"
                if message_parts:
                    message += " " + " ".join(message_parts)
                message += f".\n\n📊 Статистика БД: всего протоколов={total_count}, ожидающих={pending_count}, загружено={downloaded_count}"
                
                if pending_count == 0 and not request.force_reload:
                    message += "\n💡 Совет: Попробуйте включить 'Принудительная перезагрузка' для повторной загрузки уже загруженных протоколов."
                elif request.from_date and request.to_date:
                    # Проверяем, есть ли протоколы в этом диапазоне с любым статусом
                    date_query = {"source": "remote_mongo_direct"}
                    date_start = datetime.combine(request.from_date.date(), datetime.min.time())
                    date_end = datetime.combine(request.to_date.date(), datetime.max.time())
                    date_query["loadDate"] = {"$gte": date_start, "$lte": date_end}
                    date_range_count = collection.count_documents(date_query)
                    if date_range_count > 0:
                        message += f"\n📅 В указанном диапазоне дат найдено {date_range_count} протоколов, но они уже загружены (status='downloaded')."
                        message += "\n💡 Включите 'Принудительная перезагрузка' для их повторной загрузки."
                
                logger.warning(message)
                return DownloadResult(
                    status="success",
                    message=message,
                    processed=0,
                    downloaded=0,
                    failed=0,
                    duration=time.time() - start_time,
                    warnings=[message]
                )
            
            if request.dry_run:
                logger.info(f"DRY-RUN: Would process {len(protocols)} protocols")
                return DownloadResult(
                    status="success",
                    message=f"DRY-RUN: Would process {len(protocols)} protocols",
                    processed=len(protocols),
                    duration=time.time() - start_time
                )
            
            # Process protocols
            logger.info(f"Processing {len(protocols)} protocols...")
            processed = 0
            downloaded = 0
            failed = 0
            skipped = 0
            errors = []
            warnings = []
            
            with ThreadPoolExecutor(max_workers=self.config.protocols_concurrency) as executor:
                futures = []
                
                for protocol in protocols:
                    future = executor.submit(
                        self._process_single_protocol,
                        protocol,
                        collection,
                        skip_existing=request.skip_existing,
                        force_reload=request.force_reload,
                        max_urls_per_unit=request.max_urls_per_unit
                    )
                    futures.append(future)
                    processed += 1
                
                # Collect results
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        downloaded += result.get("downloaded", 0)
                        failed += result.get("failed", 0)
                        if result.get("skipped"):
                            skipped += 1
                        if result.get("error"):
                            errors.append(result["error"])
                    except Exception as e:
                        failed += 1
                        error_msg = f"Error processing protocol: {str(e)}"
                        errors.append(error_msg)
                        self.counters["error_types"]["protocol_processing"] = \
                            self.counters["error_types"].get("protocol_processing", 0) + 1
                        logger.error(error_msg)
            
            # Collect final statistics
            duration = time.time() - start_time
            statistics = self._collect_statistics()
            statistics["skipped_units"] = skipped
            
            # Добавляем детальную статистику о загруженных/незагруженных UNIT
            if request.from_date and request.to_date:
                try:
                    # Генерируем отчет о статусе загрузки
                    download_report = self.status_tracker.generate_download_report(
                        date_range=(request.from_date, request.to_date),
                        db_protocols=all_protocols if 'all_protocols' in locals() else None
                    )
                    statistics["download_report"] = download_report
                    statistics["total_in_db"] = download_report["summary"].get("total_in_db", 0)
                    statistics["already_downloaded"] = download_report["summary"].get("downloaded", 0)
                    statistics["pending_to_download"] = download_report["summary"].get("pending", 0)
                except Exception as e:
                    logger.warning(f"Error generating download report: {e}")
            
            result = DownloadResult(
                status="success" if failed == 0 else "partial",
                message=f"Processed {processed} protocols: {downloaded} files downloaded, {failed} failed, {skipped} skipped",
                processed=processed,
                downloaded=downloaded,
                failed=failed,
                duration=duration,
                errors=errors,
                warnings=warnings,
                statistics=statistics
            )
            
            # Log summary
            logger.info("Download processing completed:")
            logger.info(f"  Processed units: {processed}")
            logger.info(f"  Downloaded files: {downloaded}")
            logger.info(f"  Failed files: {failed}")
            logger.info(f"  Skipped units: {skipped}")
            logger.info(f"  Duration: {duration:.2f} seconds")
            
            return result
            
        except Exception as e:
            error_msg = f"Critical error during download processing: {str(e)}"
            error_details = traceback.format_exc()
            logger.error(f"{error_msg}\n{error_details}", exc_info=True)
            return DownloadResult(
                status="error",
                message=error_msg,
                duration=time.time() - start_time,
                errors=[error_msg, f"Details: {error_details}"]
            )
        finally:
            if client:
                client.close()
                logger.debug("Closed MongoDB connection")

    def get_last_download_timestamp(self) -> Optional[datetime]:
        """
        Получить время последней успешной загрузки.
        
        Проверяет:
        1. Метаданные из БД (последний обновленный протокол со статусом downloaded)
        2. unit.meta.json файлы в директориях
        
        Returns:
            datetime последней загрузки или None если не найдено
        """
        try:
            client = get_metadata_client()
            if not client:
                logger.warning("Cannot get last download timestamp: MongoDB not available")
                return None
            
            db = client[self.config.mongo.db]
            collection = db[self.config.mongo.collection]
            
            # Ищем последний протокол со статусом downloaded
            last_protocol = collection.find_one(
                {"status": "downloaded", "source": "remote_mongo_direct"},
                sort=[("updated_at", -1)]
            )
            
            if last_protocol and last_protocol.get("updated_at"):
                updated_at = last_protocol.get("updated_at")
                if isinstance(updated_at, datetime):
                    logger.debug(f"Last download timestamp from DB: {updated_at}")
                    return updated_at
                elif isinstance(updated_at, str):
                    try:
                        return datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                    except ValueError:
                        pass
            
            # Если в БД не найдено, проверяем unit.meta.json файлы
            try:
                latest_timestamp = None
                for date_dir in self.output_dir.iterdir():
                    if not date_dir.is_dir():
                        continue
                    
                    input_dir = date_dir / "Input"
                    if not input_dir.exists():
                        continue
                    
                    for unit_dir in input_dir.iterdir():
                        if not unit_dir.is_dir() or not unit_dir.name.startswith("UNIT_"):
                            continue
                        
                        meta_file = unit_dir / "unit.meta.json"
                        if meta_file.exists():
                            try:
                                with open(meta_file, 'r', encoding='utf-8') as f:
                                    meta_data = json.load(f)
                                    downloaded_at_str = meta_data.get("downloaded_at")
                                    if downloaded_at_str:
                                        # Убираем 'Z' и парсим
                                        downloaded_at_str = downloaded_at_str.replace('Z', '+00:00')
                                        downloaded_at = datetime.fromisoformat(downloaded_at_str)
                                        if latest_timestamp is None or downloaded_at > latest_timestamp:
                                            latest_timestamp = downloaded_at
                            except Exception as e:
                                logger.debug(f"Error reading meta file {meta_file}: {e}")
                                continue
                
                if latest_timestamp:
                    logger.debug(f"Last download timestamp from meta files: {latest_timestamp}")
                    return latest_timestamp
            except Exception as e:
                logger.warning(f"Error checking meta files for last download timestamp: {e}")
            
            return None
        except Exception as e:
            logger.error(f"Error getting last download timestamp: {e}")
            return None

    def get_download_summary(self) -> Dict[str, Any]:
        """
        Get a summary of download activities.
        
        Returns:
            Dictionary with download summary
        """
        return {
            "output_directory": str(self.output_dir),
            "total_files_downloaded": len(list(self.output_dir.rglob("*"))),
            "directories_created": len([d for d in self.output_dir.rglob("*") if d.is_dir()])
        }


def main():
    """Entry point for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Protocol Downloader")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of protocols to process (0 = no limit)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for downloaded files"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Create downloader
    output_dir = Path(args.output_dir) if args.output_dir else None
    downloader = EnhancedProtocolDownloader(output_dir=output_dir)
    
    try:
        # Process protocols
        result = downloader.process_pending_protocols(limit=args.limit)
        
        # Print results
        print("\n" + "="*60)
        print("📥 DOWNLOAD RESULTS")
        print("="*60)
        
        if result.status == "success":
            print("✅ SUCCESS!")
        elif result.status == "partial":
            print("⚠️  PARTIAL SUCCESS (with errors)")
        else:
            print("❌ ERROR!")
        
        print(f"   Processed protocols: {result.processed}")
        print(f"   Downloaded documents: {result.downloaded}")
        print(f"   Failed downloads: {result.failed}")
        print(f"   Duration: {result.duration:.2f} seconds")
        
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
            stats = result.statistics
            print("\n📈 Statistics:")
            
            if "file_sizes" in stats:
                fs = stats["file_sizes"]
                print(f"   File Sizes:")
                print(f"     Total: {fs['total_bytes']:,} bytes")
                print(f"     Average: {fs['average_bytes']:.0f} bytes")
                print(f"     Max: {fs['max_bytes']:,} bytes")
            
            if "download_times" in stats:
                dt = stats["download_times"]
                print(f"   Download Times:")
                print(f"     Total: {dt['total_seconds']:.2f} seconds")
                print(f"     Average: {dt['average_seconds']:.2f} seconds")
            
            if "file_types" in stats and stats["file_types"]:
                print(f"   File Types:")
                for ext, count in sorted(stats["file_types"].items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"     {ext}: {count}")
            
            if "error_types" in stats and stats["error_types"]:
                print(f"   Error Types:")
                for error_type, count in stats["error_types"].items():
                    print(f"     {error_type}: {count}")
        
        return 0 if result.status in ["success", "partial"] else 1
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())