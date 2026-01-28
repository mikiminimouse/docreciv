"""
Health checks for SyncDB component.

Provides comprehensive health checks for:
- VPN connectivity to zakupki.gov.ru
- Remote MongoDB connectivity
- Local MongoDB connectivity
- Overall system health status
"""

import os
import time
import logging
from typing import Dict, Any, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import socket
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

from ..core.config import get_config
from ..vpn_utils import get_vpn_status, is_openvpn_running, is_vpn_interface_up, check_vpn_routes, check_zakupki_access, check_remote_mongo_vpn_access

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler with formatting
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class HealthCheckResult:
    """Represents the result of a health check."""
    
    def __init__(self, name: str, status: str, message: str = "", details: Optional[Dict[str, Any]] = None):
        self.name = name
        self.status = status  # "healthy", "degraded", "unhealthy"
        self.message = message
        self.details = details or {}
        self.timestamp = time.time()
    
    def is_healthy(self) -> bool:
        """Returns True if the health check passed."""
        return self.status == "healthy"
    
    def is_degraded(self) -> bool:
        """Returns True if the health check is degraded but functional."""
        return self.status == "degraded"
    
    def is_unhealthy(self) -> bool:
        """Returns True if the health check failed."""
        return self.status == "unhealthy"


def create_session_with_retries() -> requests.Session:
    """
    Create a requests session with retry mechanism.
    
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
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    
    return session


def check_vpn_connectivity(timeout: int = 10) -> HealthCheckResult:
    """
    Check VPN connectivity to zakupki.gov.ru with comprehensive checks.
    
    Args:
        timeout: Timeout in seconds for the check
        
    Returns:
        HealthCheckResult with status and details
    """
    # Проверяем флаг VPN_ENABLED для zakupki
    vpn_enabled = os.environ.get("VPN_ENABLED_ZAKUPKI", os.environ.get("VPN_ENABLED", "false")).lower() in ("true", "1", "yes")
    vpn_required = os.environ.get("VPN_REQUIRED", "false").lower() in ("true", "1", "yes")
    
    # Если VPN не включен, но требуется - предупреждаем
    if vpn_required and not vpn_enabled:
        return HealthCheckResult(
            name="VPN Connectivity",
            status="unhealthy",
            message="VPN required but not enabled (set VPN_ENABLED_ZAKUPKI=true in .env)",
            details={
                "vpn_enabled": vpn_enabled,
                "vpn_required": vpn_required,
                "suggestion": "Установите VPN_ENABLED_ZAKUPKI=true в Configuration если VPN включен"
            }
        )
    
    # Если VPN не требуется, возвращаем успех без проверки
    if not vpn_required:
        return HealthCheckResult(
            name="VPN Connectivity",
            status="healthy",
            message="VPN not required for this system",
            details={"vpn_required": False}
        )
    
    # Получаем полный статус VPN
    vpn_status = get_vpn_status()
    
    # Формируем детальную информацию
    details = {
        "openvpn_running": vpn_status["openvpn_running"],
        "interface_up": vpn_status["interface_up"],
        "routes": vpn_status["routes_ok"],
        "vpn_enabled": vpn_enabled
    }
    
    # Проверяем OpenVPN процесс
    if not vpn_status["openvpn_running"]:
        return HealthCheckResult(
            name="VPN Connectivity",
            status="unhealthy",
            message="OpenVPN process is not running",
            details={
                **details,
                "suggestion": f"Запустите OpenVPN: sudo openvpn --config {os.environ.get('VPN_CONFIG_FILE', '/root/winners_preprocessor/final_preprocessing/receiver/vitaly_bychkov.ovpn')}"
            }
        )
    
    # Проверяем VPN интерфейс
    if not vpn_status["interface_up"]:
        return HealthCheckResult(
            name="VPN Connectivity",
            status="unhealthy",
            message="VPN interface (tun0/tap0) is not up",
            details={
                **details,
                "suggestion": "Проверьте что OpenVPN туннель установлен. Интерфейс должен быть поднят."
            }
        )
    
    # Проверяем маршруты к zakupki.gov.ru
    zakupki_routes = {k: v for k, v in vpn_status["routes_ok"].items() if "zakupki" in k.lower()}
    if not any(zakupki_routes.values()):
        logger.warning("Route to zakupki.gov.ru not found through VPN interface")
        details["route_warning"] = "Route to zakupki.gov.ru may not be configured"
    
    # Проверяем реальную доступность zakupki.gov.ru
    try:
        accessible, message, response_time = check_zakupki_access()
        
        if accessible:
            return HealthCheckResult(
                name="VPN Connectivity",
                status="healthy",
                message=f"Successfully connected to zakupki.gov.ru",
                details={
                    **details,
                    "response_time_seconds": round(response_time, 2) if response_time else None,
                    "final_url": message if isinstance(message, str) and message.startswith("http") else None
                }
            )
        else:
            return HealthCheckResult(
                name="VPN Connectivity",
                status="unhealthy",
                message=f"zakupki.gov.ru is not accessible: {message}",
                details={
                    **details,
                    "error": message,
                    "suggestion": "Проверьте что VPN подключен и маршруты настроены через route-up-zakupki.sh"
                }
            )
    except Exception as e:
        return HealthCheckResult(
            name="VPN Connectivity",
            status="unhealthy",
            message=f"Error checking zakupki.gov.ru access: {str(e)}",
            details={
                **details,
                "error": str(e),
                "suggestion": "Проверьте подключение VPN и доступность zakupki.gov.ru"
            }
        )


def check_remote_mongodb_connectivity(timeout: int = 20) -> HealthCheckResult:
    """
    Check connectivity to remote MongoDB.
    
    Args:
        timeout: Timeout in seconds for the connection attempt
        
    Returns:
        HealthCheckResult with status and details
    """
    config = get_config()
    remote_config = config.sync_db.remote_mongo
    
    # Проверяем настройки VPN для Remote MongoDB
    vpn_enabled = os.environ.get("VPN_ENABLED_REMOTE_MONGO", os.environ.get("VPN_ENABLED", "false")).lower() in ("true", "1", "yes")
    remote_mongo_use_vpn = os.environ.get("REMOTE_MONGO_USE_VPN", "true").lower() in ("true", "1", "yes")
    
    try:
        logger.debug(f"Checking remote MongoDB connectivity to {remote_config.server}")
        
        # Если требуется VPN для Remote MongoDB, но VPN не включен - предупреждаем
        if remote_mongo_use_vpn and not vpn_enabled:
            return HealthCheckResult(
                name="Remote MongoDB",
                status="unhealthy",
                message="VPN required for Remote MongoDB but VPN not enabled",
                details={
                    "server": remote_config.server,
                    "vpn_enabled": vpn_enabled,
                    "remote_mongo_use_vpn": remote_mongo_use_vpn,
                    "suggestion": "Установите VPN_ENABLED=true в Configuration если VPN активен, или установите REMOTE_MONGO_USE_VPN=false если VPN не требуется"
                }
            )
        
        # Validate required parameters
        if not remote_config.password:
            return HealthCheckResult(
                name="Remote MongoDB",
                status="unhealthy",
                message="MONGO_PASSWORD not set for remote MongoDB",
                details={"server": remote_config.server}
            )
        
        if not remote_config.ssl_cert or not os.path.exists(remote_config.ssl_cert):
            return HealthCheckResult(
                name="Remote MongoDB",
                status="unhealthy",
                message=f"SSL certificate not found: {remote_config.ssl_cert}",
                details={"ssl_cert": remote_config.ssl_cert, "server": remote_config.server}
            )
        
        # Если требуется VPN, проверяем VPN статус
        if remote_mongo_use_vpn:
            vpn_status = get_vpn_status()
            
            # Проверяем OpenVPN процесс
            if not vpn_status["openvpn_running"]:
                return HealthCheckResult(
                    name="Remote MongoDB",
                    status="unhealthy",
                    message="OpenVPN process is not running",
                    details={
                        "server": remote_config.server,
                        "vpn_enabled": vpn_enabled,
                        "remote_mongo_use_vpn": remote_mongo_use_vpn,
                        "suggestion": f"Запустите OpenVPN: sudo openvpn --config {os.environ.get('VPN_CONFIG_FILE', '/root/winners_preprocessor/final_preprocessing/receiver/vitaly_bychkov.ovpn')}"
                    }
                )
            
            # Проверяем VPN интерфейс
            if not vpn_status["interface_up"]:
                return HealthCheckResult(
                    name="Remote MongoDB",
                    status="unhealthy",
                    message="VPN interface (tun0/tap0) is not up",
                    details={
                        "server": remote_config.server,
                        "vpn_enabled": vpn_enabled,
                        "remote_mongo_use_vpn": remote_mongo_use_vpn,
                        "suggestion": "Проверьте что OpenVPN туннель установлен"
                    }
                )
            
            # Проверяем маршрут к MongoDB серверу
            mongo_host = remote_config.server.split(':')[0]
            routes = check_vpn_routes([mongo_host])
            if not routes.get(mongo_host, False):
                logger.warning(f"Route to MongoDB server {mongo_host} not found through VPN interface")
        
        # Create connection URL
        url = remote_config.get_connection_url()
        
        # Measure connection time
        start_time = time.time()
        
        # Create client with SSL settings
        client = MongoClient(
            url,
            tls=True,
            tlsCAFile=remote_config.ssl_cert,
            tlsAllowInvalidHostnames=True,
            serverSelectionTimeoutMS=timeout * 1000,
            connectTimeoutMS=timeout * 1000,
            socketTimeoutMS=timeout * 1000,
            maxPoolSize=5
        )
        
        # Test connection with ping
        client.admin.command("ping")
        
        end_time = time.time()
        connection_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # Test database access
        db = client[remote_config.db]
        collection_names = db.list_collection_names()
        
        # Close connection
        client.close()
        
        return HealthCheckResult(
            name="Remote MongoDB",
            status="healthy",
            message="Successfully connected to remote MongoDB",
            details={
                "server": remote_config.server,
                "database": remote_config.db,
                "collections": len(collection_names),
                "connection_time_ms": round(connection_time, 2),
                "auth_source": remote_config.auth_source
            }
        )
        
    except ServerSelectionTimeoutError:
        suggestion = "Проверьте подключение к Remote MongoDB через VPN"
        if remote_mongo_use_vpn and not vpn_enabled:
            suggestion = "Установите VPN_ENABLED_REMOTE_MONGO=true в Configuration если VPN активен"
        elif remote_mongo_use_vpn:
            # Добавляем детальную диагностику VPN
            vpn_status = get_vpn_status()
            vpn_details = {
                "openvpn_running": vpn_status["openvpn_running"],
                "interface_up": vpn_status["interface_up"]
            }
            
            if not vpn_status["openvpn_running"]:
                suggestion = "Запустите OpenVPN: sudo openvpn --config /root/winners_preprocessor/final_preprocessing/receiver/vitaly_bychkov.ovpn"
            elif not vpn_status["interface_up"]:
                suggestion = "Проверьте что OpenVPN туннель установлен (интерфейс не поднят)"
            else:
                # Проверяем маршрут
                mongo_host = remote_config.server.split(':')[0]
                routes = check_vpn_routes([mongo_host])
                if not routes.get(mongo_host, False):
                    suggestion = f"Маршрут к {mongo_host} не найден через VPN. Проверьте route-up-zakupki.sh"
        else:
            suggestion = "Проверьте подключение к Remote MongoDB"
        
        return HealthCheckResult(
            name="Remote MongoDB",
            status="unhealthy",
            message="Server selection timeout (VPN or MongoDB server issue)",
            details={
                "server": remote_config.server,
                "timeout_seconds": timeout,
                "vpn_enabled": vpn_enabled,
                "remote_mongo_use_vpn": remote_mongo_use_vpn,
                "suggestion": suggestion
            }
        )
    except ConnectionFailure as e:
        return HealthCheckResult(
            name="Remote MongoDB",
            status="unhealthy",
            message=f"Connection failure: {str(e)}",
            details={
                "server": remote_config.server,
                "error": str(e)
            }
        )
    except Exception as e:
        return HealthCheckResult(
            name="Remote MongoDB",
            status="unhealthy",
            message=f"Unexpected error: {str(e)}",
            details={
                "server": remote_config.server,
                "error": str(e)
            }
        )


def check_local_mongodb_connectivity(timeout: int = 10) -> HealthCheckResult:
    """
    Check connectivity to local MongoDB.
    
    Args:
        timeout: Timeout in seconds for the connection attempt
        
    Returns:
        HealthCheckResult with status and details
    """
    config = get_config()
    local_config = config.sync_db.local_mongo
    
    try:
        logger.debug(f"Checking local MongoDB connectivity to {local_config.server}")
        
        # Validate required parameters
        if not local_config.password:
            return HealthCheckResult(
                name="Local MongoDB",
                status="unhealthy",
                message="MONGO_METADATA_PASSWORD not set for local MongoDB",
                details={"server": local_config.server}
            )
        
        # Create connection URL
        url = local_config.get_connection_url()
        
        # Measure connection time
        start_time = time.time()
        
        # Create client
        client = MongoClient(
            url,
            serverSelectionTimeoutMS=timeout * 1000,
            connectTimeoutMS=timeout * 1000,
            socketTimeoutMS=timeout * 1000,
            maxPoolSize=5
        )
        
        # Test connection with ping
        client.admin.command("ping")
        
        end_time = time.time()
        connection_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        # Test database access
        db = client[local_config.db]
        collection_names = db.list_collection_names()
        
        # Close connection
        client.close()
        
        return HealthCheckResult(
            name="Local MongoDB",
            status="healthy",
            message="Successfully connected to local MongoDB",
            details={
                "server": local_config.server,
                "database": local_config.db,
                "collections": len(collection_names),
                "connection_time_ms": round(connection_time, 2),
                "auth_source": local_config.auth_source
            }
        )
        
    except ServerSelectionTimeoutError:
        return HealthCheckResult(
            name="Local MongoDB",
            status="unhealthy",
            message="Server selection timeout (local MongoDB not running?)",
            details={
                "server": local_config.server,
                "timeout_seconds": timeout,
                "suggestion": "Проверьте что MongoDB запущен: sudo systemctl start mongod или docker-compose up -d mongodb"
            }
        )
    except ConnectionFailure as e:
        error_msg = str(e)
        suggestion = "Проверьте что MongoDB запущен и доступен на указанном адресе"
        if "Connection refused" in error_msg:
            suggestion = "MongoDB не запущен. Запустите: sudo systemctl start mongod или docker-compose up -d mongodb"
        elif "Authentication failed" in error_msg:
            suggestion = "Проверьте правильность MONGO_METADATA_USER и MONGO_METADATA_PASSWORD в .env"
        
        return HealthCheckResult(
            name="Local MongoDB",
            status="unhealthy",
            message=f"Connection failure: {error_msg}",
            details={
                "server": local_config.server,
                "error": error_msg,
                "suggestion": suggestion
            }
        )
    except Exception as e:
        return HealthCheckResult(
            name="Local MongoDB",
            status="unhealthy",
            message=f"Unexpected error: {str(e)}",
            details={
                "server": local_config.server,
                "error": str(e)
            }
        )


def check_ssl_certificate_validity(cert_path: str) -> HealthCheckResult:
    """
    Check if SSL certificate file exists and is readable.
    
    Args:
        cert_path: Path to SSL certificate file
        
    Returns:
        HealthCheckResult with status and details
    """
    try:
        logger.debug(f"Checking SSL certificate validity: {cert_path}")
        
        if not os.path.exists(cert_path):
            return HealthCheckResult(
                name="SSL Certificate",
                status="unhealthy",
                message=f"SSL certificate file not found: {cert_path}",
                details={"cert_path": cert_path}
            )
        
        if not os.access(cert_path, os.R_OK):
            return HealthCheckResult(
                name="SSL Certificate",
                status="unhealthy",
                message=f"SSL certificate file not readable: {cert_path}",
                details={"cert_path": cert_path}
            )
        
        # Try to read certificate file
        with open(cert_path, 'r') as f:
            content = f.read()
            
        if len(content) < 100:  # Basic sanity check
            return HealthCheckResult(
                name="SSL Certificate",
                status="degraded",
                message=f"SSL certificate file seems too small: {len(content)} bytes",
                details={"cert_path": cert_path, "file_size": len(content)}
            )
        
        return HealthCheckResult(
            name="SSL Certificate",
            status="healthy",
            message="SSL certificate file is valid and readable",
            details={"cert_path": cert_path, "file_size": len(content)}
        )
        
    except Exception as e:
        return HealthCheckResult(
            name="SSL Certificate",
            status="unhealthy",
            message=f"Error reading SSL certificate: {str(e)}",
            details={"cert_path": cert_path, "error": str(e)}
        )


def check_environment_variables() -> HealthCheckResult:
    """
    Check if required environment variables are set.
    
    Returns:
        HealthCheckResult with status and details
    """
    try:
        logger.debug("Checking environment variables")
        
        config = get_config()
        remote_config = config.sync_db.remote_mongo
        local_config = config.sync_db.local_mongo
        
        # Check remote MongoDB credentials
        missing_remote_vars = []
        if not remote_config.password:
            missing_remote_vars.append("MONGO_PASSWORD")
        if not remote_config.ssl_cert:
            missing_remote_vars.append("MONGO_SSL_CERT")
            
        # Check local MongoDB credentials
        missing_local_vars = []
        if not local_config.password:
            missing_local_vars.append("MONGO_METADATA_PASSWORD")
            
        # Compile results
        missing_vars = missing_remote_vars + missing_local_vars
        
        if missing_vars:
            return HealthCheckResult(
                name="Environment Variables",
                status="unhealthy",
                message=f"Missing required environment variables: {', '.join(missing_vars)}",
                details={
                    "missing_remote_vars": missing_remote_vars,
                    "missing_local_vars": missing_local_vars
                }
            )
        else:
            return HealthCheckResult(
                name="Environment Variables",
                status="healthy",
                message="All required environment variables are set",
                details={
                    "remote_server": remote_config.server,
                    "local_server": local_config.server
                }
            )
            
    except Exception as e:
        return HealthCheckResult(
            name="Environment Variables",
            status="unhealthy",
            message=f"Error checking environment variables: {str(e)}",
            details={"error": str(e)}
        )


def run_comprehensive_health_check() -> Dict[str, HealthCheckResult]:
    """
    Run all health checks and return results.
    
    Returns:
        Dictionary mapping check names to HealthCheckResult objects
    """
    logger.info("Running comprehensive health check")
    
    results = {}
    
    # Run individual checks
    results["vpn"] = check_vpn_connectivity()
    results["remote_mongodb"] = check_remote_mongodb_connectivity()
    results["local_mongodb"] = check_local_mongodb_connectivity()
    results["environment"] = check_environment_variables()
    
    # Check SSL certificate specifically
    config = get_config()
    if config.sync_db.remote_mongo.ssl_cert:
        results["ssl_certificate"] = check_ssl_certificate_validity(config.sync_db.remote_mongo.ssl_cert)
    
    return results


def get_overall_system_health() -> Tuple[str, str]:
    """
    Get overall system health status.
    
    Returns:
        Tuple of (status, message) where status is one of:
        - "healthy": All checks passed
        - "degraded": Some checks degraded but system functional
        - "unhealthy": Critical checks failed
    """
    results = run_comprehensive_health_check()
    
    # Count statuses
    healthy_count = sum(1 for result in results.values() if result.is_healthy())
    degraded_count = sum(1 for result in results.values() if result.is_degraded())
    unhealthy_count = sum(1 for result in results.values() if result.is_unhealthy())
    
    total_checks = len(results)
    
    if unhealthy_count > 0:
        return "unhealthy", f"{unhealthy_count}/{total_checks} checks failed"
    elif degraded_count > 0:
        return "degraded", f"{degraded_count}/{total_checks} checks degraded"
    else:
        return "healthy", f"All {total_checks} checks passed"


def print_health_check_report(results: Dict[str, HealthCheckResult]) -> None:
    """
    Print a formatted health check report.
    
    Args:
        results: Dictionary of health check results
    """
    print("\n" + "="*60)
    print("🏥 HEALTH CHECK REPORT")
    print("="*60)
    
    # Print individual check results
    for check_name, result in results.items():
        status_icon = "✅" if result.is_healthy() else "⚠️" if result.is_degraded() else "❌"
        print(f"\n{status_icon} {result.name}")
        print(f"   Status: {result.status.upper()}")
        print(f"   Message: {result.message}")
        
        if result.details:
            print("   Details:")
            for key, value in result.details.items():
                print(f"     {key}: {value}")
    
    # Print overall status
    overall_status, overall_message = get_overall_system_health()
    status_icon = "✅" if overall_status == "healthy" else "⚠️" if overall_status == "degraded" else "❌"
    
    print(f"\n{status_icon} OVERALL SYSTEM HEALTH: {overall_status.upper()}")
    print(f"   {overall_message}")
    
    print("\n" + "="*60)


def main():
    """Entry point for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Health checks for SyncDB component")
    parser.add_argument(
        "--check",
        choices=["all", "vpn", "remote-mongo", "local-mongo", "env", "ssl"],
        default="all",
        help="Specific check to run"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Run selected check(s)
    if args.check == "all":
        results = run_comprehensive_health_check()
        print_health_check_report(results)
    elif args.check == "vpn":
        result = check_vpn_connectivity()
        print(f"VPN Check: {result.status.upper()} - {result.message}")
    elif args.check == "remote-mongo":
        result = check_remote_mongodb_connectivity()
        print(f"Remote MongoDB Check: {result.status.upper()} - {result.message}")
    elif args.check == "local-mongo":
        result = check_local_mongodb_connectivity()
        print(f"Local MongoDB Check: {result.status.upper()} - {result.message}")
    elif args.check == "env":
        result = check_environment_variables()
        print(f"Environment Variables Check: {result.status.upper()} - {result.message}")
    elif args.check == "ssl":
        config = get_config()
        if config.sync_db.remote_mongo.ssl_cert:
            result = check_ssl_certificate_validity(config.sync_db.remote_mongo.ssl_cert)
            print(f"SSL Certificate Check: {result.status.upper()} - {result.message}")
        else:
            print("SSL Certificate Check: SKIPPED - No SSL certificate configured")
    
    # Return appropriate exit code
    if args.check == "all":
        overall_status, _ = get_overall_system_health()
        return 0 if overall_status == "healthy" else 1
    else:
        # For individual checks, we'd need to determine the result
        # This is simplified for now
        return 0


if __name__ == "__main__":
    exit(main())