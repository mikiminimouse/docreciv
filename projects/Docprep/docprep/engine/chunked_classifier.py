"""
Chunked Classifier - классификация файлов с chunked processing и recovery.

Решает проблему потери 89% данных путем обработки файлов порциями
с поддержкой восстановления после прерываний.
"""

import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from .classifier import Classifier
from ..core.chunk_manager import ChunkManager, ChunkStatus
from ..core.recovery_engine import RecoveryEngine, RecoveryResult
from ..core.pipeline_monitor import PipelineMonitor
from ..core.circuit_breaker import PipelineCircuitBreaker, CircuitBreakerOpenException


logger = logging.getLogger(__name__)


class ChunkedClassifier:
    """
    Классификатор с chunked processing для решения проблемы потери данных.

    Особенности:
    - Обработка файлов порциями (чанками)
    - Recovery после прерываний
    - Progress tracking
    - Graceful error handling
    """

    def __init__(self, state_dir: Path, chunk_size: int = 100, enable_monitoring: bool = True):
        """
        Инициализация Chunked Classifier.

        Args:
            state_dir: Директория для сохранения состояния
            chunk_size: Размер чанка (файлов)
            enable_monitoring: Включить мониторинг и circuit breaker
        """
        self.chunk_manager = ChunkManager(state_dir, chunk_size)
        self.recovery_engine = RecoveryEngine(self.chunk_manager)
        self.classifier = Classifier()

        # Мониторинг и защита (Phase 3)
        self.enable_monitoring = enable_monitoring
        if enable_monitoring:
            self.monitor = PipelineMonitor(state_dir / "monitoring")
            self.circuit_breaker = PipelineCircuitBreaker()
        else:
            self.monitor = None
            self.circuit_breaker = None

        # Статистика сессии
        self.session_stats = {
            "start_time": None,
            "end_time": None,
            "chunks_processed": 0,
            "files_processed": 0,
            "files_failed": 0,
            "errors": []
        }

        monitoring_status = "with monitoring" if enable_monitoring else "without monitoring"
        logger.info(f"🔧 ChunkedClassifier initialized (chunk_size={chunk_size}, {monitoring_status})")

    def classify_with_recovery(
        self,
        input_files: List[Path],
        cycle: int,
        protocol_date: Optional[str] = None,
        protocol_id: Optional[str] = None,
        force_recreate_chunks: bool = False,
        max_processing_time: int = 3600,  # 1 час
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Классифицирует файлы с chunked processing и recovery.

        Args:
            input_files: Список файлов для классификации
            cycle: Номер цикла
            protocol_date: Дата протокола
            protocol_id: ID протокола
            force_recreate_chunks: Пересоздать чанки принудительно
            max_processing_time: Максимальное время обработки (секунды)
            dry_run: Режим проверки

        Returns:
            Результаты классификации
        """
        self.session_stats["start_time"] = datetime.now()
        start_time = time.time()

        try:
            logger.info(f"🚀 Начинаем chunked классификацию {len(input_files)} файлов")
            logger.info(f"📊 Chunk size: {self.chunk_manager.chunk_size}")
            logger.info(f"📅 Cycle: {cycle}, Date: {protocol_date}")

            # Инициализируем мониторинг (Phase 3)
            if self.enable_monitoring and self.monitor:
                self.monitor.start_pipeline(len(input_files), f"chunked_cycle_{cycle}")
                self.monitor.record_stage_start("chunk_creation")

            # Создаем чанки
            chunks = self.chunk_manager.create_chunks(input_files, force_recreate_chunks)

            # Recovery check - восстанавливаем состояние если нужно
            recovery_result = self.recovery_engine.perform_recovery()
            if recovery_result.recovered_chunks > 0:
                logger.info(f"🔄 Восстановлено {recovery_result.recovered_chunks} чанков")

            # Обрабатываем чанки
            processed_chunks = 0
            total_processed = 0
            total_failed = 0

            while time.time() - start_time < max_processing_time:
                chunk = self.chunk_manager.get_next_chunk()

                if chunk is None:
                    # Все чанки обработаны
                    break

                logger.info(f"📦 Обрабатываем чанк {chunk.id} ({len(chunk.files)} файлов)")

                # Мониторинг: начало обработки чанка
                if self.enable_monitoring and self.monitor:
                    self.monitor.record_stage_start(f"chunk_{chunk.id}")

                try:
                    chunk_start_time = time.time()

                    # Обрабатываем чанк через circuit breaker
                    if self.enable_monitoring and self.circuit_breaker:
                        chunk_result = self.circuit_breaker.protect_chunk_processing(
                            self._process_chunk,
                            chunk, cycle, protocol_date, protocol_id, dry_run
                        )
                    else:
                        chunk_result = self._process_chunk(
                            chunk, cycle, protocol_date, protocol_id, dry_run
                        )

                    chunk_processing_time = time.time() - chunk_start_time

                    # Обновляем статистику чанка
                    self.chunk_manager.update_chunk_progress(
                        chunk.id,
                        chunk_result["processed"],
                        chunk_result["failed"],
                        chunk_result.get("error")
                    )

                    # Мониторинг: запись результатов обработки чанка
                    if self.enable_monitoring and self.monitor:
                        chunk_success = chunk_result["failed"] == 0
                        self.monitor.record_file_processed(
                            filename=f"chunk_{chunk.id}",
                            success=chunk_success,
                            processing_time=chunk_processing_time,
                            file_type="chunk",
                            stage=f"chunk_processing",
                            error=chunk_result.get("error") if not chunk_success else None
                        )
                        self.monitor.record_stage_end(f"chunk_{chunk.id}", chunk_processing_time)

                    # Определяем финальный статус чанка
                    if chunk_result["failed"] == 0:
                        final_status = ChunkStatus.SUCCESS
                        logger.info(f"✅ Чанк {chunk.id} завершен успешно ({chunk_processing_time:.1f}s)")
                    else:
                        final_status = ChunkStatus.FAILED
                        logger.warning(f"⚠️  Чанк {chunk.id} завершен с ошибками: {chunk_result['failed']} неудач")

                    self.chunk_manager.mark_chunk_completed(
                        chunk.id, final_status, chunk_result.get("error")
                    )

                    processed_chunks += 1
                    total_processed += chunk_result["processed"]
                    total_failed += chunk_result["failed"]

                    # Промежуточная статистика
                    self._log_progress_stats()

                except Exception as e:
                    error_msg = f"Критическая ошибка обработки чанка {chunk.id}: {e}"
                    logger.error(f"❌ {error_msg}")

                    self.chunk_manager.mark_chunk_completed(
                        chunk.id, ChunkStatus.FAILED, error_msg
                    )

                    total_failed += len(chunk.files)
                    self.session_stats["errors"].append(error_msg)

                    # Проверяем, можем ли retry
                    if not self.chunk_manager.retry_chunk(chunk.id):
                        logger.error(f"❌ Превышен лимит retry для чанка {chunk.id}")

            # Финальная статистика
            self.session_stats.update({
                "end_time": datetime.now(),
                "chunks_processed": processed_chunks,
                "files_processed": total_processed,
                "files_failed": total_failed,
                "processing_time_seconds": time.time() - start_time
            })

            # Финальный отчет
            final_stats = self.chunk_manager.get_processing_stats()

            result = {
                "success": final_stats["files"]["completion_percentage"] >= 95.0,  # 95%+ успех
                "stats": final_stats,
                "session_stats": self.session_stats,
                "processing_time": time.time() - start_time,
                "chunks_created": len(chunks),
                "chunks_processed": processed_chunks,
                "recovery_needed": recovery_result.recovered_chunks > 0,
                "errors": self.session_stats["errors"]
            }

            # Завершаем мониторинг (Phase 3)
            if self.enable_monitoring and self.monitor:
                self.monitor.end_pipeline()

            self._log_final_report(result)
            return result

        except Exception as e:
            error_msg = f"Критическая ошибка chunked классификации: {e}"
            logger.error(f"💥 {error_msg}")

            self.session_stats.update({
                "end_time": datetime.now(),
                "errors": self.session_stats["errors"] + [error_msg]
            })

            return {
                "success": False,
                "error": error_msg,
                "session_stats": self.session_stats,
                "processing_time": time.time() - start_time
            }

    def _process_chunk(
        self,
        chunk: Any,  # Chunk из chunk_manager
        cycle: int,
        protocol_date: Optional[str],
        protocol_id: Optional[str],
        dry_run: bool
    ) -> Dict[str, Any]:
        """
        Обрабатывает один чанк файлов.

        Args:
            chunk: Чанк для обработки
            cycle: Номер цикла
            protocol_date: Дата протокола
            protocol_id: ID протокола
            dry_run: Режим проверки

        Returns:
            Результаты обработки чанка
        """
        processed = 0
        failed = 0
        errors = []

        for file_path in chunk.files:
            try:
                # Определяем UNIT директорию для файла
                unit_dir = self._get_unit_directory(file_path)

                if unit_dir and unit_dir.exists():
                    # Классифицируем UNIT
                    result = self.classifier.classify_unit(
                        unit_path=unit_dir,
                        cycle=cycle,
                        protocol_date=protocol_date,
                        protocol_id=protocol_id,
                        dry_run=dry_run
                    )

                    # Проверяем успех классификации:
                    # - Есть поле "moved_to" (успешное перемещение)
                    # - Нет поля "error"
                    # - Есть поле "category"
                    if (result.get("moved_to") and not result.get("error") and result.get("category")):
                        processed += 1
                        logger.debug(f"✓ Обработан: {unit_dir.name} -> {result.get('category')}")
                    else:
                        failed += 1
                        error_msg = f"Неудачная классификация: {unit_dir.name}"
                        if result.get("error"):
                            error_msg += f" ({result['error']})"
                        errors.append(error_msg)
                        logger.warning(f"⚠️  {error_msg}")
                else:
                    failed += 1
                    error_msg = f"UNIT директория не найдена: {file_path}"
                    errors.append(error_msg)
                    logger.error(f"❌ {error_msg}")

            except Exception as e:
                failed += 1
                error_msg = f"Ошибка обработки файла {file_path.name}: {e}"
                errors.append(error_msg)
                logger.error(f"❌ {error_msg}")

        return {
            "processed": processed,
            "failed": failed,
            "total": len(chunk.files),
            "errors": errors,
            "error": "; ".join(errors[:3]) if errors else None  # Первые 3 ошибки
        }

    def _get_unit_directory(self, file_path: Path) -> Optional[Path]:
        """
        Определяет UNIT директорию для файла.

        Args:
            file_path: Путь к файлу

        Returns:
            Путь к UNIT директории или None
        """
        # Сначала проверим, находится ли файл уже в UNIT директории
        current = file_path.parent
        while current != current.parent:  # Пока не корень
            if current.name.startswith("UNIT_"):
                return current
            current = current.parent

        # Если не нашли, попробуем найти UNIT директорию по имени файла
        # Файлы могут быть в структуре: input_dir/UNIT_xxx/filename
        file_stem = file_path.stem  # Получаем имя файла без расширения

        # Ищем директорию с таким же именем (без расширения) в родительской директории
        parent_dir = file_path.parent
        potential_unit_dir = parent_dir / file_stem

        if potential_unit_dir.exists() and potential_unit_dir.is_dir() and potential_unit_dir.name.startswith("UNIT_"):
            return potential_unit_dir

        # Альтернативный подход: ищем все UNIT директории в родительской директории
        # и ищем ту, которая содержит этот файл
        for item in parent_dir.iterdir():
            if item.is_dir() and item.name.startswith("UNIT_"):
                # Проверяем, содержит ли эта UNIT директория наш файл
                if file_path.name in [f.name for f in item.glob("*") if f.is_file()]:
                    return item

        logger.warning(f"Не удалось определить UNIT директорию для файла: {file_path}")
        return None

    def _log_progress_stats(self):
        """Логирует промежуточную статистику прогресса."""
        stats = self.chunk_manager.get_processing_stats()

        logger.info(
            f"📊 Прогресс: {stats['chunks']['completed']}/{stats['chunks']['total']} чанков "
            f"({stats['chunks']['completion_percentage']:.1f}%), "
            f"{stats['files']['processed']}/{stats['files']['total']} файлов "
            f"({stats['files']['completion_percentage']:.1f}%)"
        )

    def _log_final_report(self, result: Dict[str, Any]):
        """Логирует финальный отчет."""
        session = result["session_stats"]

        logger.info("🎯 Классификация завершена:")
        logger.info(f"   ⏱️  Время обработки: {session['processing_time_seconds']:.1f} сек")
        logger.info(f"   📦 Чанков обработано: {result['chunks_processed']}")
        logger.info(f"   📄 Файлов обработано: {session['files_processed']}")
        logger.info(f"   ❌ Ошибок: {session['files_failed']}")

        if result["success"]:
            logger.info("✅ Классификация успешна!")
        else:
            logger.warning("⚠️  Классификация завершена с ошибками")

        if result.get("errors"):
            logger.warning(f"🚨 Критические ошибки: {len(result['errors'])}")

    def get_status_report(self) -> Dict[str, Any]:
        """
        Возвращает отчет о текущем состоянии.

        Returns:
            Подробный статус
        """
        stats = self.chunk_manager.get_processing_stats()
        recovery_report = self.recovery_engine.get_recovery_report()

        report = {
            "chunk_stats": stats,
            "recovery_status": recovery_report,
            "session_stats": self.session_stats,
            "is_active": self.session_stats["start_time"] is not None and self.session_stats["end_time"] is None,
            "needs_recovery": recovery_report["recovery_needed"]
        }

        # Добавляем данные Phase 3 (monitoring & circuit breaker)
        if self.enable_monitoring:
            if self.monitor:
                report["pipeline_monitor"] = self.monitor.get_progress_report()
                report["performance_report"] = self.monitor.get_performance_report()

            if self.circuit_breaker:
                report["circuit_breaker_status"] = self.circuit_breaker.get_overall_status()

        return report

    def emergency_stop(self):
        """Экстренная остановка обработки."""
        logger.warning("🚨 Emergency stop requested!")

        # Сбрасываем все processing чанки
        reset_count = 0
        for chunk in self.chunk_manager:
            if chunk.status == ChunkStatus.PROCESSING:
                chunk.status = ChunkStatus.PENDING
                chunk.error_message = "Emergency stop"
                reset_count += 1

        if reset_count > 0:
            self.chunk_manager._save_state()
            logger.info(f"✅ Emergency stop: сброшено {reset_count} чанков")

    def cleanup_state(self):
        """Очищает состояние для fresh start."""
        self.chunk_manager.reset_processing()
        self.session_stats = {
            "start_time": None,
            "end_time": None,
            "chunks_processed": 0,
            "files_processed": 0,
            "files_failed": 0,
            "errors": []
        }
        logger.info("🧹 Состояние очищено для fresh start")