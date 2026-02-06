"""
Pipeline Monitor - система мониторинга и observability для preprocessing pipeline.

Обеспечивает:
- Real-time monitoring обработки
- Progress tracking и метрики
- Error reporting и alerting
- Performance metrics collection
"""

import time
import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Метрики pipeline обработки."""
    start_time: float = 0.0
    end_time: Optional[float] = None
    total_files: int = 0
    processed_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0

    # По стадиям
    stage_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # По типам файлов
    file_type_metrics: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Ошибки
    error_counts: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    recent_errors: List[Dict[str, Any]] = field(default_factory=list)

    # Производительность
    avg_processing_time: float = 0.0
    peak_memory_usage: int = 0
    total_processing_time: float = 0.0

    @property
    def completion_percentage(self) -> float:
        """Процент завершения обработки."""
        if self.total_files == 0:
            return 100.0
        processed = self.processed_files + self.failed_files + self.skipped_files
        return (processed / self.total_files) * 100

    @property
    def success_rate(self) -> float:
        """Процент успешной обработки."""
        total_processed = self.processed_files + self.failed_files
        if total_processed == 0:
            return 100.0
        return (self.processed_files / total_processed) * 100

    @property
    def error_rate(self) -> float:
        """Процент ошибок обработки."""
        total_processed = self.processed_files + self.failed_files
        if total_processed == 0:
            return 0.0
        return (self.failed_files / total_processed) * 100

    @property
    def elapsed_time(self) -> float:
        """Общее затраченное время."""
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time

    @property
    def estimated_completion_time(self) -> Optional[float]:
        """Оценка времени до завершения."""
        if self.completion_percentage >= 100:
            return 0.0

        elapsed = self.elapsed_time
        if elapsed == 0 or self.completion_percentage == 0:
            return None

        total_estimated = elapsed / (self.completion_percentage / 100)
        return total_estimated - elapsed

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь."""
        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "failed_files": self.failed_files,
            "skipped_files": self.skipped_files,
            "completion_percentage": self.completion_percentage,
            "success_rate": self.success_rate,
            "error_rate": self.error_rate,
            "elapsed_time": self.elapsed_time,
            "estimated_completion_time": self.estimated_completion_time,
            "stage_metrics": dict(self.stage_metrics),
            "file_type_metrics": dict(self.file_type_metrics),
            "error_counts": dict(self.error_counts),
            "avg_processing_time": self.avg_processing_time,
            "peak_memory_usage": self.peak_memory_usage,
            "total_processing_time": self.total_processing_time,
        }


class PipelineMonitor:
    """
    Монитор preprocessing pipeline.

    Отвечает за:
    - Сбор метрик обработки
    - Progress tracking
    - Error monitoring и alerting
    - Performance analysis
    """

    def __init__(self, log_dir: Path, enable_alerts: bool = True):
        """
        Инициализация монитора.

        Args:
            log_dir: Директория для логов и метрик
            enable_alerts: Включить алерты при проблемах
        """
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.enable_alerts = enable_alerts
        self.metrics = PipelineMetrics()

        # Файлы для сохранения состояния
        self.metrics_file = log_dir / "pipeline_metrics.json"
        self.alerts_file = log_dir / "alerts.log"

        # Настройка логирования
        self._setup_monitoring_logger()

        # Загрузка предыдущего состояния если есть
        self._load_previous_metrics()

        logger.info(f"🔧 PipelineMonitor initialized (log_dir={log_dir})")

    def _setup_monitoring_logger(self):
        """Настройка специального логгера для мониторинга."""
        self.monitor_logger = logging.getLogger("pipeline_monitor")
        self.monitor_logger.setLevel(logging.INFO)

        # Удаляем существующие handlers
        for handler in self.monitor_logger.handlers[:]:
            self.monitor_logger.removeHandler(handler)

        # File handler для метрик
        metrics_handler = logging.FileHandler(self.log_dir / "pipeline_monitor.log")
        metrics_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(stage)s - %(message)s'
        ))
        self.monitor_logger.addHandler(metrics_handler)

        # Console handler для важных событий
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '📊 %(levelname)s - %(stage)s - %(message)s'
        ))
        # Показывать только WARNING и выше в консоль
        console_handler.setLevel(logging.WARNING)
        self.monitor_logger.addHandler(console_handler)

    def _load_previous_metrics(self):
        """Загрузка предыдущих метрик если доступны."""
        if self.metrics_file.exists():
            try:
                with open(self.metrics_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Восстанавливаем основные метрики
                self.metrics.start_time = data.get('start_time', time.time())
                self.metrics.total_files = data.get('total_files', 0)
                self.metrics.processed_files = data.get('processed_files', 0)
                self.metrics.failed_files = data.get('failed_files', 0)
                self.metrics.skipped_files = data.get('skipped_files', 0)
                logger.info("📂 Previous metrics loaded from checkpoint")
            except Exception as e:
                logger.warning(f"Could not load previous metrics: {e}")

    def start_pipeline(self, total_files: int, pipeline_id: str = "default"):
        """
        Начало мониторинга pipeline.

        Args:
            total_files: Общее количество файлов для обработки
            pipeline_id: ID pipeline для идентификации
        """
        self.metrics = PipelineMetrics()
        self.metrics.start_time = time.time()
        self.metrics.total_files = total_files

        self.monitor_logger.info(
            f"Pipeline started: {pipeline_id} with {total_files} files",
            extra={"stage": "start", "pipeline_id": pipeline_id}
        )

        logger.info(f"🚀 Pipeline monitoring started for {total_files} files")

    def record_file_processed(self, filename: str, success: bool, processing_time: float = 0.0,
                            file_type: str = "", stage: str = "", error: Optional[str] = None):
        """
        Запись обработки файла.

        Args:
            filename: Имя файла
            success: Успешно ли обработан
            processing_time: Время обработки в секундах
            file_type: Тип файла (расширение)
            stage: Стадия обработки
            error: Сообщение об ошибке если есть
        """
        if success:
            self.metrics.processed_files += 1
        else:
            self.metrics.failed_files += 1

        # Обновляем метрики по типам файлов
        if file_type:
            self.metrics.file_type_metrics[file_type] += 1

        # Обновляем метрики по стадиям
        if stage:
            if stage not in self.metrics.stage_metrics:
                self.metrics.stage_metrics[stage] = {
                    "processed": 0, "failed": 0, "total_time": 0.0, "avg_time": 0.0
                }
            stage_stats = self.metrics.stage_metrics[stage]
            stage_stats["processed" if success else "failed"] += 1
            stage_stats["total_time"] += processing_time

            total_in_stage = stage_stats["processed"] + stage_stats["failed"]
            if total_in_stage > 0:
                stage_stats["avg_time"] = stage_stats["total_time"] / total_in_stage

        # Обновляем метрики производительности
        if processing_time > 0:
            self.metrics.total_processing_time += processing_time
            total_processed = self.metrics.processed_files + self.metrics.failed_files
            if total_processed > 0:
                self.metrics.avg_processing_time = self.metrics.total_processing_time / total_processed

        # Записываем ошибку если есть
        if error:
            self.metrics.error_counts[error] += 1
            error_record = {
                "timestamp": datetime.now().isoformat(),
                "filename": filename,
                "stage": stage,
                "error": error,
                "file_type": file_type
            }
            self.metrics.recent_errors.append(error_record)

            # Оставляем только последние 100 ошибок
            if len(self.metrics.recent_errors) > 100:
                self.metrics.recent_errors = self.metrics.recent_errors[-100:]

            # Алерт при высокой частоте ошибок
            if self.enable_alerts and self.metrics.error_rate > 20.0:
                self._alert_high_error_rate()

        # Периодически сохраняем метрики
        if (self.metrics.processed_files + self.metrics.failed_files) % 10 == 0:
            self._save_metrics_checkpoint()

    def record_stage_start(self, stage_name: str):
        """Запись начала стадии обработки."""
        self.monitor_logger.info(
            f"Stage started: {stage_name}",
            extra={"stage": stage_name}
        )

    def record_stage_end(self, stage_name: str, duration: float):
        """Запись завершения стадии обработки."""
        self.monitor_logger.info(
            ".2f",
            extra={"stage": stage_name, "duration": duration}
        )

    def end_pipeline(self):
        """Завершение мониторинга pipeline."""
        self.metrics.end_time = time.time()

        # Финальный отчет
        self._generate_final_report()

        # Сохранение финальных метрик
        self._save_metrics_checkpoint()

        self.monitor_logger.info(
            "Pipeline completed",
            extra={"stage": "end", "duration": self.metrics.elapsed_time}
        )

        logger.info(f"🏁 Pipeline completed in {self.metrics.elapsed_time:.1f}s")
    def get_progress_report(self) -> Dict[str, Any]:
        """
        Получение отчета о прогрессе.

        Returns:
            Подробный отчет о текущем состоянии
        """
        return {
            "timestamp": datetime.now().isoformat(),
            "metrics": self.metrics.to_dict(),
            "status": "active" if not self.metrics.end_time else "completed",
            "alerts": self._check_alerts(),
        }

    def get_performance_report(self) -> Dict[str, Any]:
        """
        Получение отчета о производительности.

        Returns:
            Анализ производительности
        """
        report = {
            "overall_performance": {
                "total_files": self.metrics.total_files,
                "completion_percentage": self.metrics.completion_percentage,
                "success_rate": self.metrics.success_rate,
                "error_rate": self.metrics.error_rate,
                "elapsed_time": self.metrics.elapsed_time,
                "avg_processing_time": self.metrics.avg_processing_time,
                "estimated_completion": self.metrics.estimated_completion_time,
            },
            "stage_performance": {},
            "file_type_distribution": dict(self.metrics.file_type_metrics),
            "top_errors": sorted(
                self.metrics.error_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
        }

        # Анализ по стадиям
        for stage, stats in self.metrics.stage_metrics.items():
            report["stage_performance"][stage] = {
                "efficiency": (stats["processed"] / (stats["processed"] + stats["failed"])) * 100
                if (stats["processed"] + stats["failed"]) > 0 else 0,
                "avg_time": stats["avg_time"],
                "total_processed": stats["processed"],
                "total_failed": stats["failed"]
            }

        return report

    def _check_alerts(self) -> List[str]:
        """Проверка условий для алертов."""
        alerts = []

        # Высокий процент ошибок
        if self.metrics.error_rate > 15.0:
            alerts.append(f"High error rate: {self.metrics.error_rate:.1f}%")

        # Долгое время обработки
        if self.metrics.elapsed_time > 3600 and self.metrics.completion_percentage < 50:
            alerts.append(f"Slow progress: {self.metrics.completion_percentage:.1f}% in {self.metrics.elapsed_time:.0f}s")

        # Много пропущенных файлов
        if self.metrics.skipped_files > self.metrics.total_files * 0.1:
            alerts.append(f"High skip rate: {self.metrics.skipped_files}/{self.metrics.total_files} files")

        return alerts

    def _alert_high_error_rate(self):
        """Алерты при высокой частоте ошибок."""
        if not self.enable_alerts:
            return

        alert_msg = f"🚨 HIGH ERROR RATE: {self.metrics.error_rate:.1f}% ({self.metrics.failed_files}/{self.metrics.processed_files + self.metrics.failed_files})"

        # Логируем алерт
        with open(self.alerts_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - {alert_msg}\n")

        self.monitor_logger.warning(alert_msg, extra={"stage": "alert"})

    def _save_metrics_checkpoint(self):
        """Сохранение метрик в checkpoint файл."""
        try:
            with open(self.metrics_file, 'w', encoding='utf-8') as f:
                json.dump(self.metrics.to_dict(), f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Could not save metrics checkpoint: {e}")

    def _generate_final_report(self):
        """Генерация финального отчета."""
        report_file = self.log_dir / "final_report.json"

        report = {
            "pipeline_summary": self.metrics.to_dict(),
            "performance_analysis": self.get_performance_report(),
            "error_analysis": {
                "total_errors": self.metrics.failed_files,
                "error_rate": self.metrics.error_rate,
                "top_errors": self.metrics.error_counts,
                "recent_errors": self.metrics.recent_errors[-10:]  # Последние 10 ошибок
            },
            "stage_analysis": dict(self.metrics.stage_metrics),
            "file_type_analysis": dict(self.metrics.file_type_metrics),
            "alerts": self._check_alerts(),
            "generated_at": datetime.now().isoformat()
        }

        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            logger.info(f"📋 Final report saved: {report_file}")
        except Exception as e:
            logger.error(f"Could not save final report: {e}")

    def __str__(self) -> str:
        """Строковое представление текущего состояния."""
        return (f"PipelineMonitor("
                f"files={self.metrics.processed_files}/{self.metrics.total_files} "
                f"({self.metrics.completion_percentage:.1f}%), "
                f"success={self.metrics.success_rate:.1f}%, "
                f"errors={self.metrics.failed_files})")