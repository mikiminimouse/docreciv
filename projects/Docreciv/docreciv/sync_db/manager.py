"""
SyncManagerService - оркестратор синхронизации базы данных.

Управляет синхронизацией протоколов с поддержкой различных режимов:
- incremental: от последнего курсора до текущей даты
- range: синхронизация произвольного диапазона дат
- backfill: догрузка исторических данных (не обновляет курсор)
- replay: переигрывание уже синхронизированного периода
"""

import logging
import uuid
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Callable, List, Tuple
from dataclasses import asdict

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from ..core.config import get_config
from .models import (
    SyncRequest, SyncCursorState, SyncProgressEvent,
    SyncRunResult, SyncRunHandle, SyncRunStatus
)
from .enhanced_service import EnhancedSyncService, EnhancedSyncResult

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler with formatting
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class SyncManagerService:
    """
    Сервис-оркестратор для управления синхронизацией базы данных.
    
    Обеспечивает управляемую синхронизацию с поддержкой различных режимов,
    отслеживанием прогресса и управлением курсорами.
    """
    
    def __init__(self):
        """Инициализация SyncManagerService."""
        self.config = get_config()
        self.sync_service = EnhancedSyncService()
        self.client = None
        self.sync_runs_collection = None
        self.cursors_collection = None
        
        # Активные запуски (run_id -> thread)
        self._active_runs: Dict[str, threading.Thread] = {}
        self._run_cancelled: Dict[str, bool] = {}
        self._run_lock = threading.Lock()
        
        # Callback для событий прогресса
        self._progress_callbacks: Dict[str, Callable[[SyncProgressEvent], None]] = {}
        
        logger.info("SyncManagerService initialized")
    
    def _get_mongo_client(self) -> Optional[MongoClient]:
        """
        Получить клиент MongoDB для хранения метаданных синхронизации.
        
        Returns:
            MongoClient или None при ошибке подключения
        """
        if self.client:
            return self.client
        
        try:
            local_config = self.config.sync_db.local_mongo
            
            url = local_config.get_connection_url()
            
            self.client = MongoClient(
                url,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
                maxPoolSize=10
            )
            
            # Тест подключения
            self.client.admin.command("ping")
            
            # Получить коллекции
            db = self.client[local_config.db]
            self.sync_runs_collection = db["sync_runs"]
            self.cursors_collection = db["sync_cursors"]
            
            # Создать индексы
            self._ensure_indexes()
            
            logger.info("Connected to MongoDB for sync metadata")
            return self.client
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB for sync metadata: {e}")
            return None
    
    def _ensure_indexes(self) -> None:
        """Создать необходимые индексы в MongoDB."""
        try:
            if self.sync_runs_collection is not None:
                # Индексы для sync_runs
                self.sync_runs_collection.create_index([("run_id", 1)], unique=True, name="run_id_idx")
                self.sync_runs_collection.create_index([("status", 1)], name="status_idx")
                self.sync_runs_collection.create_index([("created_at", -1)], name="created_at_idx")
                self.sync_runs_collection.create_index([("collection", 1), ("mode", 1)], name="collection_mode_idx")
            
            if self.cursors_collection is not None:
                # Индексы для cursors
                self.cursors_collection.create_index([("collection", 1)], unique=True, name="collection_idx")
                self.cursors_collection.create_index([("last_successful_run", -1)], name="last_run_idx")
                
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")
    
    def _get_cursor_state(self, collection: str) -> Optional[SyncCursorState]:
        """
        Получить состояние курсора для коллекции.
        
        Args:
            collection: Имя коллекции
            
        Returns:
            SyncCursorState или None если курсор не найден
        """
        try:
            client = self._get_mongo_client()
            if not client:
                return None
            
            cursor_doc = self.cursors_collection.find_one({"collection": collection})
            
            if cursor_doc:
                return SyncCursorState.from_dict(cursor_doc)
            
            # Создать начальное состояние курсора
            initial_cursor = SyncCursorState(
                collection=collection,
                cursor_field="loadDate",
                last_cursor_value=datetime(2019, 1, 1),  # Начальная дата
                last_successful_run=datetime.utcnow()
            )
            self._save_cursor_state(initial_cursor)
            return initial_cursor
            
        except Exception as e:
            logger.error(f"Failed to get cursor state: {e}")
            return None
    
    def _save_cursor_state(self, cursor_state: SyncCursorState) -> bool:
        """
        Сохранить состояние курсора.
        
        Args:
            cursor_state: Состояние курсора для сохранения
            
        Returns:
            True если успешно, False иначе
        """
        try:
            client = self._get_mongo_client()
            if not client:
                return False
            
            self.cursors_collection.replace_one(
                {"collection": cursor_state.collection},
                cursor_state.to_dict(),
                upsert=True
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save cursor state: {e}")
            return False
    
    def _update_run_status(self, run_id: str, status: str, **kwargs) -> bool:
        """
        Обновить статус запуска синхронизации.
        
        Args:
            run_id: Идентификатор запуска
            status: Новый статус
            **kwargs: Дополнительные поля для обновления
            
        Returns:
            True если успешно, False иначе
        """
        try:
            client = self._get_mongo_client()
            if not client:
                return False
            
            update_data = {
                "status": status,
                "updated_at": datetime.utcnow(),
                **kwargs
            }
            
            self.sync_runs_collection.update_one(
                {"run_id": run_id},
                {"$set": update_data}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update run status: {e}")
            return False
    
    def _emit_progress(self, event: SyncProgressEvent) -> None:
        """
        Эмитировать событие прогресса.
        
        Args:
            event: Событие прогресса
        """
        callback = self._progress_callbacks.get(event.run_id)
        if callback:
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Error in progress callback: {e}")
    
    def _calculate_date_range(self, request: SyncRequest) -> Tuple[datetime, datetime]:
        """
        Вычислить диапазон дат для синхронизации на основе режима.
        
        Args:
            request: Запрос синхронизации
            
        Returns:
            Кортеж (start_date, end_date)
        """
        now = datetime.utcnow()
        
        if request.mode == "incremental":
            # От последнего курсора до текущей даты
            cursor_state = self._get_cursor_state(request.collection)
            if cursor_state:
                start_date = cursor_state.last_cursor_value
            else:
                # Если курсор не найден, начинаем с вчерашнего дня
                start_date = now - timedelta(days=1)
            end_date = now
            
        elif request.mode == "range":
            # Произвольный диапазон
            start_date = request.from_date
            end_date = request.to_date
            
        elif request.mode == "backfill":
            # От from_date до последнего курсора (не обновляем курсор)
            cursor_state = self._get_cursor_state(request.collection)
            if cursor_state:
                end_date = cursor_state.last_cursor_value
            else:
                end_date = now
            start_date = request.from_date
            
        elif request.mode == "replay":
            # Переигрывание диапазона
            start_date = request.from_date
            end_date = request.to_date
            
        else:
            raise ValueError(f"Unknown sync mode: {request.mode}")
        
        return start_date, end_date
    
    def _run_sync(self, run_id: str, request: SyncRequest) -> None:
        """
        Выполнить синхронизацию в отдельном потоке.
        
        Args:
            run_id: Идентификатор запуска
            request: Запрос синхронизации
        """
        try:
            # Обновить статус на "running"
            self._update_run_status(run_id, "running", started_at=datetime.utcnow())
            
            # Вычислить диапазон дат
            start_date, end_date = self._calculate_date_range(request)
            
            logger.info(f"Starting sync run {run_id}: {request.mode} from {start_date.date()} to {end_date.date()}")
            
            # Эмитировать начальное событие прогресса
            self._emit_progress(SyncProgressEvent(
                run_id=run_id,
                processed=0,
                current_cursor=start_date
            ))
            
            # Выполнить синхронизацию
            if request.mode == "incremental":
                # Для incremental используем sync_protocols_for_date_range
                result = self.sync_service.sync_protocols_for_date_range(
                    start_date=start_date,
                    end_date=end_date,
                    limit=None
                )
            elif request.mode in ["range", "backfill", "replay"]:
                # Для остальных режимов также используем sync_protocols_for_date_range
                result = self.sync_service.sync_protocols_for_date_range(
                    start_date=start_date,
                    end_date=end_date,
                    limit=None
                )
            else:
                raise ValueError(f"Unknown sync mode: {request.mode}")
            
            # Проверить на отмену
            if self._run_cancelled.get(run_id, False):
                self._update_run_status(run_id, "cancelled", finished_at=datetime.utcnow())
                logger.info(f"Sync run {run_id} was cancelled")
                return
            
            # Обновить курсор только для incremental и range (не для backfill)
            if request.mode in ["incremental", "range"] and not request.dry_run:
                cursor_state = self._get_cursor_state(request.collection)
                if cursor_state:
                    cursor_state.last_cursor_value = end_date
                    cursor_state.last_successful_run = datetime.utcnow()
                    self._save_cursor_state(cursor_state)
            
            # Создать результат
            sync_result = SyncRunResult(
                run_id=run_id,
                success=result.status in ["success", "partial"],
                started_at=datetime.utcnow() - timedelta(seconds=result.duration),
                finished_at=datetime.utcnow(),
                stats={
                    "scanned": result.scanned,
                    "inserted": result.inserted,
                    "skipped": result.skipped_existing,
                    "errors": result.errors_count,
                    "duration": result.duration,
                    "statistics": result.statistics
                },
                errors=result.errors or [],
                warnings=result.warnings or []
            )
            
            # Сохранить результат
            self._update_run_status(
                run_id,
                "completed" if sync_result.success else "failed",
                finished_at=sync_result.finished_at,
                current_cursor=end_date,
                stats=sync_result.stats,
                errors=sync_result.errors,
                warnings=sync_result.warnings
            )
            
            # Эмитировать финальное событие прогресса
            self._emit_progress(SyncProgressEvent(
                run_id=run_id,
                processed=result.scanned,
                inserted=result.inserted,
                skipped=result.skipped_existing,
                errors=result.errors_count,
                current_cursor=end_date
            ))
            
            logger.info(f"Sync run {run_id} completed: {result.status}")
            
        except Exception as e:
            logger.error(f"Error in sync run {run_id}: {e}", exc_info=True)
            self._update_run_status(
                run_id,
                "failed",
                finished_at=datetime.utcnow(),
                errors=[str(e)]
            )
        finally:
            # Удалить из активных запусков
            with self._run_lock:
                if run_id in self._active_runs:
                    del self._active_runs[run_id]
                if run_id in self._run_cancelled:
                    del self._run_cancelled[run_id]
                if run_id in self._progress_callbacks:
                    del self._progress_callbacks[run_id]
    
    def start_sync(self, request: SyncRequest, progress_callback: Optional[Callable[[SyncProgressEvent], None]] = None) -> SyncRunHandle:
        """
        Запустить синхронизацию.
        
        Args:
            request: Запрос синхронизации
            progress_callback: Опциональный callback для событий прогресса
            
        Returns:
            SyncRunHandle для отслеживания запуска
        """
        # Валидация запроса
        if request.collection != "protocols":
            raise ValueError(f"Only 'protocols' collection is supported, got: {request.collection}")
        
        # Генерировать run_id
        run_id = str(uuid.uuid4())
        
        # Создать дескриптор запуска
        handle = SyncRunHandle(
            run_id=run_id,
            status="pending",
            request=request
        )
        
        # Сохранить в MongoDB
        try:
            client = self._get_mongo_client()
            if client:
                self.sync_runs_collection.insert_one(handle.to_dict())
        except Exception as e:
            logger.error(f"Failed to save sync run handle: {e}")
        
        # Сохранить callback если предоставлен
        if progress_callback:
            self._progress_callbacks[run_id] = progress_callback
        
        # Запустить синхронизацию в отдельном потоке
        thread = threading.Thread(
            target=self._run_sync,
            args=(run_id, request),
            daemon=True
        )
        
        with self._run_lock:
            self._active_runs[run_id] = thread
            self._run_cancelled[run_id] = False
        
        thread.start()
        
        logger.info(f"Started sync run {run_id} with mode {request.mode}")
        
        return handle
    
    def get_status(self, run_id: str) -> Optional[SyncRunStatus]:
        """
        Получить статус запуска синхронизации.
        
        Args:
            run_id: Идентификатор запуска
            
        Returns:
            SyncRunStatus или None если запуск не найден
        """
        try:
            client = self._get_mongo_client()
            if not client:
                return None
            
            run_doc = self.sync_runs_collection.find_one({"run_id": run_id})
            
            if not run_doc:
                return None
            
            # Вычислить прогресс
            stats = run_doc.get("stats", {})
            scanned = stats.get("scanned", 0)
            inserted = stats.get("inserted", 0)
            skipped = stats.get("skipped", 0)
            errors = stats.get("errors", 0)
            
            status_value = run_doc.get("status", "unknown")
            
            # Вычислить прогресс
            progress = 0.0
            if status_value == "completed":
                progress = 100.0
            elif status_value == "failed":
                progress = 0.0
            elif status_value == "running":
                # Для running статуса прогресс вычисляется на основе обработанных документов
                # Но так как мы не знаем общее количество заранее, используем приблизительную оценку
                if scanned > 0:
                    # Предполагаем, что если обработано много, то прогресс высокий
                    progress = min(95.0, (scanned / 10000) * 100.0)  # Примерная оценка
                else:
                    progress = 5.0  # Минимальный прогресс для running
            
            return SyncRunStatus(
                run_id=run_id,
                status=status_value,
                progress=progress,
                processed=scanned,  # processed = scanned (все обработанные документы)
                inserted=inserted,
                skipped=skipped,
                errors=errors,
                current_cursor=run_doc.get("current_cursor"),
                message=f"Status: {status_value}"
            )
            
        except Exception as e:
            logger.error(f"Failed to get sync status: {e}")
            return None
    
    def cancel(self, run_id: str) -> bool:
        """
        Отменить запуск синхронизации.
        
        Args:
            run_id: Идентификатор запуска
            
        Returns:
            True если успешно отменен, False иначе
        """
        with self._run_lock:
            if run_id in self._active_runs:
                self._run_cancelled[run_id] = True
                self._update_run_status(run_id, "cancelled")
                logger.info(f"Cancelled sync run {run_id}")
                return True
            else:
                logger.warning(f"Sync run {run_id} not found or already completed")
                return False
    
    def get_cursor_state(self, collection: str) -> Optional[SyncCursorState]:
        """
        Получить состояние курсора для коллекции.
        
        Args:
            collection: Имя коллекции
            
        Returns:
            SyncCursorState или None
        """
        return self._get_cursor_state(collection)
    
    def get_recent_runs(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получить список последних запусков синхронизации.
        
        Args:
            limit: Максимальное количество запусков
            
        Returns:
            Список словарей с информацией о запусках
        """
        try:
            client = self._get_mongo_client()
            if not client:
                return []
            
            runs = list(self.sync_runs_collection.find(
                {},
                sort=[("created_at", -1)],
                limit=limit
            ))
            
            # Преобразовать ObjectId в строку
            for run in runs:
                if "_id" in run:
                    run["_id"] = str(run["_id"])
            
            return runs
            
        except Exception as e:
            logger.error(f"Failed to get recent runs: {e}")
            return []
    
    def close(self) -> None:
        """Закрыть все подключения."""
        if self.client:
            self.client.close()
            self.client = None
        if self.sync_service:
            self.sync_service.close()

