"""
Recovery Engine для восстановления обработки после сбоев.

Обеспечивает:
- Автоматическое восстановление прерванной обработки
- Graceful handling ошибок
- State persistence между запусками
- Progress tracking и reporting
"""

import time
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Any, Callable
from datetime import datetime, timedelta

from .chunk_manager import ChunkManager, Chunk, ChunkStatus


@dataclass
class RecoveryResult:
    """Результат операции recovery."""
    recovered_chunks: int = 0
    failed_chunks: int = 0
    skipped_chunks: int = 0
    total_chunks: int = 0
    recovery_time_seconds: float = 0.0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class RecoveryEngine:
    """
    Двигатель восстановления для chunk-based обработки.

    Отвечает за:
    - Анализ состояния после сбоя
    - Восстановление незавершенных чанков
    - Координацию retry логики
    - Progress reporting
    """

    def __init__(self, chunk_manager: ChunkManager, max_recovery_time: int = 300):
        """
        Инициализация Recovery Engine.

        Args:
            chunk_manager: Менеджер чанков
            max_recovery_time: Максимальное время на recovery (секунды)
        """
        self.chunk_manager = chunk_manager
        self.max_recovery_time = max_recovery_time

        # Настройка логирования
        self.logger = logging.getLogger("recovery_engine")
        self.logger.setLevel(logging.INFO)

        # Создаем handler если не существует
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def analyze_system_state(self) -> Dict[str, Any]:
        """
        Анализирует текущее состояние системы.

        Returns:
            Подробный анализ состояния
        """
        stats = self.chunk_manager.get_processing_stats()

        # Анализируем незавершенные чанки
        incomplete_chunks = []
        stuck_chunks = []
        failed_chunks = []

        for chunk in self.chunk_manager:
            if chunk.status == ChunkStatus.PROCESSING:
                # Проверяем, не завис ли чанк
                if chunk.started_at:
                    processing_time = datetime.now() - chunk.started_at
                    if processing_time > timedelta(hours=1):  # Зависший чанк
                        stuck_chunks.append(chunk)
                    else:
                        incomplete_chunks.append(chunk)
            elif chunk.status == ChunkStatus.FAILED:
                failed_chunks.append(chunk)

        return {
            "stats": stats,
            "incomplete_chunks": len(incomplete_chunks),
            "stuck_chunks": len(stuck_chunks),
            "failed_chunks": len(failed_chunks),
            "recovery_needed": len(incomplete_chunks) + len(stuck_chunks) + len(failed_chunks) > 0,
            "stuck_chunk_ids": [c.id for c in stuck_chunks],
            "failed_chunk_ids": [c.id for c in failed_chunks]
        }

    def perform_recovery(self, force_retry_failed: bool = False) -> RecoveryResult:
        """
        Выполняет восстановление системы.

        Args:
            force_retry_failed: Принудительно retry неудачные чанки

        Returns:
            Результат recovery операции
        """
        start_time = time.time()
        result = RecoveryResult()

        self.logger.info("🔄 Начинаем recovery процесс...")

        try:
            # Анализируем состояние
            state_analysis = self.analyze_system_state()
            result.total_chunks = state_analysis["stats"]["chunks"]["total"]

            self.logger.info(f"📊 Найдено чанков: {result.total_chunks}")
            self.logger.info(f"📈 Завершено: {state_analysis['stats']['chunks']['completed']}")
            self.logger.info(f"⏳ Незавершено: {state_analysis['incomplete_chunks']}")
            self.logger.info(f"❌ Зависших: {len(state_analysis['stuck_chunk_ids'])}")
            self.logger.info(f"💥 Провалившихся: {len(state_analysis['failed_chunk_ids'])}")

            # Восстанавливаем незавершенные чанки
            recovered_count = 0

            # 1. Сбрасываем зависшие чанки
            for chunk_id in state_analysis["stuck_chunk_ids"]:
                try:
                    chunk = self.chunk_manager[chunk_id]
                    self.logger.warning(f"🔄 Сбрасываем зависший чанк {chunk_id} (время обработки: {datetime.now() - chunk.started_at})")

                    # Сбрасываем статус
                    chunk.status = ChunkStatus.PENDING
                    chunk.started_at = None
                    chunk.completed_at = None
                    chunk.error_message = "Reset due to stuck processing"
                    recovered_count += 1

                except Exception as e:
                    self.logger.error(f"❌ Ошибка сброса чанка {chunk_id}: {e}")
                    result.errors.append(f"Failed to reset stuck chunk {chunk_id}: {e}")

            # 2. Retry неудачных чанков (опционально)
            if force_retry_failed:
                for chunk_id in state_analysis["failed_chunk_ids"]:
                    try:
                        if self.chunk_manager.retry_chunk(chunk_id):
                            self.logger.info(f"🔄 Retry чанка {chunk_id}")
                            recovered_count += 1
                        else:
                            self.logger.warning(f"❌ Превышен лимит retry для чанка {chunk_id}")

                    except Exception as e:
                        self.logger.error(f"❌ Ошибка retry чанка {chunk_id}: {e}")
                        result.errors.append(f"Failed to retry chunk {chunk_id}: {e}")

            # 3. Сбрасываем processing чанки (могут быть из предыдущего запуска)
            for chunk in self.chunk_manager:
                if chunk.status == ChunkStatus.PROCESSING:
                    try:
                        self.logger.info(f"🔄 Сбрасываем незавершенный чанк {chunk.id}")
                        chunk.status = ChunkStatus.PENDING
                        chunk.started_at = None
                        chunk.completed_at = None
                        chunk.error_message = "Reset from previous incomplete run"
                        recovered_count += 1

                    except Exception as e:
                        self.logger.error(f"❌ Ошибка сброса чанка {chunk.id}: {e}")
                        result.errors.append(f"Failed to reset chunk {chunk.id}: {e}")

            result.recovered_chunks = recovered_count
            result.recovery_time_seconds = time.time() - start_time

            # Финальная статистика
            final_stats = self.chunk_manager.get_processing_stats()
            self.logger.info("✅ Recovery завершен:")
            self.logger.info(f"   🔄 Восстановлено чанков: {result.recovered_chunks}")
            self.logger.info(f"   ⏱️  Время recovery: {result.recovery_time_seconds:.1f}s")
            self.logger.info(f"   📊 Готово к обработке: {final_stats['chunks']['total'] - final_stats['chunks']['completed']} чанков")

            return result

        except Exception as e:
            result.errors.append(f"Recovery failed: {e}")
            result.recovery_time_seconds = time.time() - start_time
            self.logger.error(f"❌ Критическая ошибка recovery: {e}")
            return result

    def validate_recovery(self) -> Dict[str, Any]:
        """
        Валидирует результат recovery.

        Returns:
            Результаты валидации
        """
        validation_results = {
            "is_valid": True,
            "issues": [],
            "recommendations": []
        }

        try:
            stats = self.chunk_manager.get_processing_stats()

            # Проверяем, что нет зависших чанков
            stuck_chunks = []
            for chunk in self.chunk_manager:
                if chunk.status == ChunkStatus.PROCESSING and chunk.started_at:
                    processing_time = datetime.now() - chunk.started_at
                    if processing_time > timedelta(minutes=30):
                        stuck_chunks.append(chunk.id)

            if stuck_chunks:
                validation_results["issues"].append(f"Обнаружены зависшие чанки: {stuck_chunks}")
                validation_results["recommendations"].append("Запустите recovery для сброса зависших чанков")

            # Проверяем, что есть чанки для обработки
            pending_chunks = sum(1 for c in self.chunk_manager if c.status == ChunkStatus.PENDING)
            if pending_chunks == 0 and stats["chunks"]["completed"] < stats["chunks"]["total"]:
                validation_results["issues"].append("Нет чанков для обработки, но не все чанки завершены")
                validation_results["recommendations"].append("Проверьте статусы чанков или пересоздайте чанки")

            # Проверяем консистентность данных
            total_chunked_files = sum(len(c.files) for c in self.chunk_manager)
            if "total_files" in self.chunk_manager.metadata:
                expected_files = self.chunk_manager.metadata["total_files"]
                if total_chunked_files != expected_files:
                    validation_results["issues"].append(
                        f"Несоответствие количества файлов: ожидается {expected_files}, в чанках {total_chunked_files}"
                    )
                    validation_results["recommendations"].append("Пересоздайте чанки с корректным списком файлов")

            validation_results["is_valid"] = len(validation_results["issues"]) == 0

        except Exception as e:
            validation_results["is_valid"] = False
            validation_results["issues"].append(f"Ошибка валидации: {e}")

        return validation_results

    def get_recovery_report(self) -> Dict[str, Any]:
        """
        Генерирует отчет о recovery состоянии.

        Returns:
            Подробный отчет
        """
        state_analysis = self.analyze_system_state()
        validation = self.validate_recovery()

        return {
            "timestamp": datetime.now().isoformat(),
            "system_state": state_analysis,
            "validation": validation,
            "recovery_needed": state_analysis["recovery_needed"],
            "recommendations": validation.get("recommendations", []) + [
                "Запустите recovery если есть незавершенные чанки",
                "Проверьте логи для анализа ошибок",
                "Рассмотрите увеличение chunk_size при частых таймаутах"
            ] if state_analysis["recovery_needed"] else []
        }

    def emergency_reset(self, confirm: bool = False) -> bool:
        """
        Emergency reset всей обработки.

        Args:
            confirm: Подтверждение операции

        Returns:
            True если reset выполнен
        """
        if not confirm:
            self.logger.warning("🚨 Emergency reset требует явного подтверждения (confirm=True)")
            return False

        try:
            self.logger.warning("🚨 Выполняется emergency reset всей обработки!")

            # Сбрасываем все чанки
            self.chunk_manager.reset_processing()

            # Очищаем метаданные recovery
            self.logger.warning("✅ Emergency reset завершен - все чанки сброшены в PENDING")
            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка emergency reset: {e}")
            return False