"""
Event-driven система для связи компонентов.

Предоставляет типизированные события и event bus для слабой связанности.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, Callable, List
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Типы событий в системе."""
    PROTOCOL_SYNCED = "protocol_synced"
    FILE_DOWNLOADED = "file_downloaded"
    FILE_PROCESSED = "file_processed"
    UNIT_CREATED = "unit_created"
    UNIT_DISTRIBUTED = "unit_distributed"
    ERROR_OCCURRED = "error_occurred"


@dataclass
class Event:
    """Базовое событие."""
    event_type: EventType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProtocolSyncedEvent(Event):
    """Событие синхронизации протокола."""
    protocol_id: str
    unit_id: str
    date: str
    
    def __post_init__(self):
        self.event_type = EventType.PROTOCOL_SYNCED
        self.context.update({
            "protocol_id": self.protocol_id,
            "unit_id": self.unit_id,
            "date": self.date
        })


@dataclass
class FileDownloadedEvent(Event):
    """Событие загрузки файла."""
    unit_id: str
    file_count: int
    file_paths: List[str]
    
    def __post_init__(self):
        self.event_type = EventType.FILE_DOWNLOADED
        self.context.update({
            "unit_id": self.unit_id,
            "file_count": self.file_count,
            "file_paths": self.file_paths
        })


@dataclass
class FileProcessedEvent(Event):
    """Событие обработки файла."""
    unit_id: str
    file_path: str
    category: str
    
    def __post_init__(self):
        self.event_type = EventType.FILE_PROCESSED
        self.context.update({
            "unit_id": self.unit_id,
            "file_path": self.file_path,
            "category": self.category
        })


@dataclass
class ErrorEvent(Event):
    """Событие ошибки."""
    error_message: str
    error_type: str
    context: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        self.event_type = EventType.ERROR_OCCURRED
        self.context.update({
            "error_message": self.error_message,
            "error_type": self.error_type
        })


class EventBus:
    """
    Event bus для публикации и подписки на события.
    
    Реализует простой паттерн Observer для слабой связанности компонентов.
    """
    
    def __init__(self):
        self._subscribers: Dict[EventType, List[Callable[[Event], None]]] = {}
    
    def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """
        Подписывается на события определенного типа.
        
        Args:
            event_type: Тип события
            handler: Функция-обработчик события
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        
        self._subscribers[event_type].append(handler)
        logger.debug(f"Subscribed handler to {event_type.value}")
    
    def publish(self, event: Event) -> None:
        """
        Публикует событие, вызывая все подписанные обработчики.
        
        Args:
            event: Событие для публикации
        """
        event_type = event.event_type
        
        if event_type not in self._subscribers:
            logger.debug(f"No subscribers for {event_type.value}")
            return
        
        handlers = self._subscribers[event_type]
        logger.debug(f"Publishing {event_type.value} to {len(handlers)} handlers")
        
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Error in event handler for {event_type.value}: {e}", exc_info=True)
    
    def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        """
        Отписывается от событий определенного типа.
        
        Args:
            event_type: Тип события
            handler: Функция-обработчик для удаления
        """
        if event_type in self._subscribers:
            try:
                self._subscribers[event_type].remove(handler)
                logger.debug(f"Unsubscribed handler from {event_type.value}")
            except ValueError:
                logger.warning(f"Handler not found in subscribers for {event_type.value}")


# Глобальный экземпляр event bus
_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """Получает глобальный экземпляр event bus."""
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus()
    return _event_bus


def set_event_bus(event_bus: EventBus) -> None:
    """Устанавливает event bus (для тестирования)."""
    global _event_bus
    _event_bus = event_bus

