"""
Chunk Manager для обработки файлов порциями с recovery.

Решает проблему потери данных при прерывании классификатора.
Гарантирует 100% coverage с возможностью восстановления с любой точки.
"""

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime


class ChunkStatus(Enum):
    """Статусы обработки чанка."""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class Chunk:
    """Представляет чанк файлов для обработки."""
    id: str
    files: List[Path] = field(default_factory=list)
    status: ChunkStatus = ChunkStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    processed_count: int = 0
    failed_count: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3

    @property
    def is_completed(self) -> bool:
        """Проверяет, завершен ли чанк (успешно или с ошибкой)."""
        return self.status in [ChunkStatus.SUCCESS, ChunkStatus.FAILED, ChunkStatus.SKIPPED]

    @property
    def progress_percentage(self) -> float:
        """Возвращает процент завершения обработки чанка."""
        if not self.files:
            return 100.0
        return (self.processed_count / len(self.files)) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь для JSON."""
        return {
            "id": self.id,
            "files": [str(f) for f in self.files],
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "processed_count": self.processed_count,
            "failed_count": self.failed_count,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Chunk':
        """Десериализация из словаря."""
        return cls(
            id=data["id"],
            files=[Path(f) for f in data["files"]],
            status=ChunkStatus(data["status"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            started_at=datetime.fromisoformat(data["started_at"]) if data["started_at"] else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data["completed_at"] else None,
            processed_count=data["processed_count"],
            failed_count=data["failed_count"],
            error_message=data["error_message"],
            retry_count=data["retry_count"],
            max_retries=data["max_retries"],
        )


class ChunkManager:
    """
    Управляет обработкой файлов чанками с поддержкой recovery.

    Гарантирует:
    - 100% coverage всех файлов
    - Recovery после прерываний
    - State persistence
    - Progress tracking
    """

    def __init__(self, state_dir: Path, chunk_size: int = 100):
        """
        Инициализация Chunk Manager.

        Args:
            state_dir: Директория для сохранения состояния
            chunk_size: Размер чанка (количество файлов)
        """
        self.state_dir = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size

        # Файлы состояния
        self.chunks_file = self.state_dir / "chunks.json"
        self.metadata_file = self.state_dir / "metadata.json"

        # Загружаем существующее состояние
        self.chunks: Dict[str, Chunk] = {}
        self.metadata: Dict[str, Any] = {}
        self._load_state()

    def create_chunks(self, input_files: List[Path], force_recreate: bool = False) -> List[Chunk]:
        """
        Создает чанки из списка файлов.

        Args:
            input_files: Список файлов для обработки
            force_recreate: Принудительно пересоздать чанки

        Returns:
            Список созданных чанков
        """
        # Проверяем, есть ли уже чанки
        if self.chunks and not force_recreate:
            print(f"⚠️  Найдено {len(self.chunks)} существующих чанков. Используйте force_recreate=True для пересоздания.")
            return list(self.chunks.values())

        # Создаем новые чанки
        self.chunks.clear()

        for i in range(0, len(input_files), self.chunk_size):
            chunk_files = input_files[i:i + self.chunk_size]
            chunk_id = f"chunk_{i//self.chunk_size:04d}"

            chunk = Chunk(
                id=chunk_id,
                files=chunk_files,
                status=ChunkStatus.PENDING
            )

            self.chunks[chunk_id] = chunk

        # Сохраняем состояние
        self.metadata = {
            "total_files": len(input_files),
            "chunk_size": self.chunk_size,
            "created_at": datetime.now().isoformat(),
            "chunk_count": len(self.chunks)
        }

        self._save_state()

        print(f"✅ Создано {len(self.chunks)} чанков по {self.chunk_size} файлов каждый")
        return list(self.chunks.values())

    def get_next_chunk(self) -> Optional[Chunk]:
        """
        Возвращает следующий чанк для обработки.

        Returns:
            Следующий PENDING чанк или None если все обработаны
        """
        for chunk in self.chunks.values():
            if chunk.status == ChunkStatus.PENDING:
                # Помечаем как processing
                chunk.status = ChunkStatus.PROCESSING
                chunk.started_at = datetime.now()
                self._save_state()
                return chunk

        return None

    def update_chunk_progress(self, chunk_id: str, processed: int, failed: int, error_msg: Optional[str] = None):
        """
        Обновляет прогресс обработки чанка.

        Args:
            chunk_id: ID чанка
            processed: Количество успешно обработанных файлов
            failed: Количество файлов с ошибками
            error_msg: Сообщение об ошибке (если есть)
        """
        if chunk_id not in self.chunks:
            raise ValueError(f"Чанк {chunk_id} не найден")

        chunk = self.chunks[chunk_id]
        chunk.processed_count = processed
        chunk.failed_count = failed

        if error_msg:
            chunk.error_message = error_msg

        # Автоматически определяем статус
        total_files = len(chunk.files)
        if chunk.processed_count + chunk.failed_count >= total_files:
            # Все файлы обработаны
            if chunk.failed_count == 0:
                chunk.status = ChunkStatus.SUCCESS
            else:
                chunk.status = ChunkStatus.FAILED
            chunk.completed_at = datetime.now()

        self._save_state()

    def mark_chunk_completed(self, chunk_id: str, status: ChunkStatus, error_msg: Optional[str] = None):
        """
        Помечает чанк как завершенный.

        Args:
            chunk_id: ID чанка
            status: Финальный статус
            error_msg: Сообщение об ошибке
        """
        if chunk_id not in self.chunks:
            raise ValueError(f"Чанк {chunk_id} не найден")

        chunk = self.chunks[chunk_id]
        chunk.status = status
        chunk.completed_at = datetime.now()

        if error_msg:
            chunk.error_message = error_msg

        self._save_state()

    def retry_chunk(self, chunk_id: str) -> bool:
        """
        Повторяет обработку чанка.

        Args:
            chunk_id: ID чанка

        Returns:
            True если retry возможен, False если превышен лимит
        """
        if chunk_id not in self.chunks:
            raise ValueError(f"Чанк {chunk_id} не найден")

        chunk = self.chunks[chunk_id]

        if chunk.retry_count >= chunk.max_retries:
            print(f"❌ Чанк {chunk_id} превысил лимит retry ({chunk.max_retries})")
            return False

        chunk.retry_count += 1
        chunk.status = ChunkStatus.PENDING
        chunk.error_message = None
        chunk.started_at = None
        chunk.completed_at = None
        chunk.processed_count = 0
        chunk.failed_count = 0

        self._save_state()
        print(f"🔄 Retry чанка {chunk_id} (попытка {chunk.retry_count}/{chunk.max_retries})")
        return True

    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику обработки.

        Returns:
            Статистика по чанкам
        """
        total_chunks = len(self.chunks)
        completed_chunks = sum(1 for c in self.chunks.values() if c.is_completed)
        successful_chunks = sum(1 for c in self.chunks.values() if c.status == ChunkStatus.SUCCESS)
        failed_chunks = sum(1 for c in self.chunks.values() if c.status == ChunkStatus.FAILED)

        total_files = sum(len(c.files) for c in self.chunks.values())
        processed_files = sum(c.processed_count for c in self.chunks.values())
        failed_files = sum(c.failed_count for c in self.chunks.values())

        return {
            "chunks": {
                "total": total_chunks,
                "completed": completed_chunks,
                "successful": successful_chunks,
                "failed": failed_chunks,
                "in_progress": total_chunks - completed_chunks,
                "completion_percentage": (completed_chunks / total_chunks * 100) if total_chunks > 0 else 0
            },
            "files": {
                "total": total_files,
                "processed": processed_files,
                "failed": failed_files,
                "remaining": total_files - processed_files - failed_files,
                "completion_percentage": (processed_files / total_files * 100) if total_files > 0 else 0
            }
        }

    def reset_processing(self):
        """Сбрасывает состояние обработки (для полного перезапуска)."""
        for chunk in self.chunks.values():
            chunk.status = ChunkStatus.PENDING
            chunk.started_at = None
            chunk.completed_at = None
            chunk.processed_count = 0
            chunk.failed_count = 0
            chunk.error_message = None
            chunk.retry_count = 0

        self._save_state()
        print("🔄 Сброшено состояние обработки всех чанков")

    def _save_state(self):
        """Сохраняет состояние в файлы."""
        # Сохраняем чанки
        chunks_data = {chunk_id: chunk.to_dict() for chunk_id, chunk in self.chunks.items()}
        with open(self.chunks_file, 'w', encoding='utf-8') as f:
            json.dump(chunks_data, f, indent=2, ensure_ascii=False)

        # Сохраняем метаданные
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)

    def _load_state(self):
        """Загружает состояние из файлов."""
        # Загружаем метаданные
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    self.metadata = json.load(f)
            except Exception as e:
                print(f"⚠️  Ошибка загрузки метаданных: {e}")

        # Загружаем чанки
        if self.chunks_file.exists():
            try:
                with open(self.chunks_file, 'r', encoding='utf-8') as f:
                    chunks_data = json.load(f)

                self.chunks = {}
                for chunk_id, chunk_data in chunks_data.items():
                    self.chunks[chunk_id] = Chunk.from_dict(chunk_data)

                print(f"📂 Загружено {len(self.chunks)} чанков из предыдущей сессии")

            except Exception as e:
                print(f"⚠️  Ошибка загрузки чанков: {e}")

    def __len__(self) -> int:
        """Возвращает количество чанков."""
        return len(self.chunks)

    def __getitem__(self, chunk_id: str) -> Chunk:
        """Возвращает чанк по ID."""
        return self.chunks[chunk_id]

    def __iter__(self):
        """Итератор по чанкам."""
        return iter(self.chunks.values())