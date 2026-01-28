"""
Определение задач для scheduler.

Содержит все задачи, которые выполняются по расписанию.
"""
import logging
import requests
from typing import Optional
from pathlib import Path
import sys

from ..core.config import get_config
from ..core.exceptions import ConnectionError, SyncError

logger = logging.getLogger(__name__)


def trigger_processing() -> None:
    """
    Вызывает endpoint router для обработки всех файлов.
    
    Raises:
        ConnectionError: При ошибке подключения к router API
    """
    config = get_config()
    router_api = config.scheduler.router_api
    
    try:
        logger.info(f"Triggering processing via {router_api}")
        response = requests.post(router_api, timeout=300)
        
        if response.status_code == 200:
            result = response.json()
            processed_count = result.get('processed_count', 0)
            logger.info(f"Processing triggered successfully: {processed_count} files processed")
        else:
            error_msg = f"Error triggering processing: {response.status_code} - {response.text}"
            logger.error(error_msg)
            raise ConnectionError(
                error_msg,
                context={
                    "router_api": router_api,
                    "status_code": response.status_code,
                    "response_text": response.text
                }
            )
    
    except requests.exceptions.Timeout as e:
        error_msg = f"Timeout while triggering processing: {e}"
        logger.error(error_msg)
        raise ConnectionError(
            error_msg,
            context={"router_api": router_api},
            original_error=e
        )
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error while triggering processing: {e}"
        logger.error(error_msg)
        raise ConnectionError(
            error_msg,
            context={"router_api": router_api},
            original_error=e
        )


def sync_protocols() -> None:
    """
    Выполняет ежедневную синхронизацию протоколов через sync_db сервис.
    
    Raises:
        SyncError: При ошибке синхронизации
    """
    try:
        logger.info("Starting daily protocol synchronization...")
        
        # Импортируем SyncService
        # Добавляем путь к проекту для импорта
        project_root = Path(__file__).parent.parent.parent.parent  # Теперь на уровень выше
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        
        from sync_db.service import SyncService
        
        # Создаем сервис синхронизации
        sync_service = SyncService()
        
        # Выполняем ежедневную синхронизацию
        result = sync_service.sync_daily_updates()
        
        if result.status in ["success", "partial"]:
            logger.info(
                f"Protocol sync completed: {result.scanned} scanned, "
                f"{result.inserted} inserted, {result.skipped_existing} skipped"
            )
        else:
            error_msg = f"Protocol sync failed: {result.message}"
            logger.error(error_msg)
            raise SyncError(
                error_msg,
                context={
                    "scanned": result.scanned,
                    "inserted": result.inserted,
                    "skipped_existing": result.skipped_existing,
                    "errors_count": result.errors_count
                }
            )
            
    except ImportError as e:
        error_msg = "Failed to import SyncService from sync_db"
        logger.error(error_msg)
        raise SyncError(
            error_msg,
            context={"import_error": str(e)},
            original_error=e
        )
    except Exception as e:
        error_msg = f"Error during protocol synchronization: {e}"
        logger.error(error_msg)
        raise SyncError(
            error_msg,
            original_error=e
        )

