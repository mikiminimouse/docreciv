"""
Scheduler для периодического запуска обработки документов.
Использует APScheduler для cron-like планирования.
"""
import logging
from apscheduler.schedulers.blocking import BlockingScheduler

from ..core.config import get_config
from .jobs import trigger_processing, sync_protocols
from .config import create_processing_trigger, create_sync_trigger

# Настройка логирования
config = get_config()
logging.basicConfig(
    level=config.scheduler.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

scheduler = BlockingScheduler()

# Добавляем задачу для обработки документов
processing_trigger = create_processing_trigger()
scheduler.add_job(
    trigger_processing,
    trigger=processing_trigger,
    id="process_documents",
    name="Process documents from input directory"
)

# Добавляем задачу для ежедневной синхронизации протоколов
sync_trigger = create_sync_trigger()
scheduler.add_job(
    sync_protocols,
    trigger=sync_trigger,
    id="sync_protocols",
    name="Daily protocol synchronization"
)


if __name__ == "__main__":
    try:
        logger.info("Scheduler started. Press Ctrl+C to exit.")
        logger.info("Jobs configured:")
        for job in scheduler.get_jobs():
            logger.info(f"  - {job.name} ({job.id})")
        
        # Запускаем планировщик
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
        scheduler.shutdown()