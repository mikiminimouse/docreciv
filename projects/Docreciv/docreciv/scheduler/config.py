"""
Конфигурация scheduler.
"""
import logging
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from typing import Optional, Union

from ..core.config import get_config
from ..core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def create_processing_trigger() -> Union[CronTrigger, IntervalTrigger]:
    """
    Создает trigger для задачи обработки документов.
    
    Returns:
        CronTrigger или IntervalTrigger в зависимости от конфигурации
    
    Raises:
        ConfigurationError: При невалидной конфигурации cron
    """
    config = get_config()
    schedule_cron = config.scheduler.schedule_cron
    
    try:
        parts = schedule_cron.split()
        
        if len(parts) == 5:
            minute, hour, day, month, day_of_week = parts
            
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week
            )
            
            logger.info(f"Scheduler configured with cron for processing: {schedule_cron}")
            return trigger
        else:
            # Если формат неверный, используем интервал (каждые 15 минут)
            logger.warning(f"Invalid cron format: {schedule_cron}. Using default interval (15 minutes)")
            return IntervalTrigger(minutes=15)
    
    except Exception as e:
        logger.error(f"Error configuring scheduler for processing: {e}")
        logger.info("Using default interval (15 minutes) for processing")
        return IntervalTrigger(minutes=15)


def create_sync_trigger() -> Union[CronTrigger, IntervalTrigger]:
    """
    Создает trigger для задачи синхронизации протоколов.
    
    Returns:
        CronTrigger или IntervalTrigger в зависимости от конфигурации
    
    Raises:
        ConfigurationError: При невалидной конфигурации cron
    """
    config = get_config()
    sync_schedule_cron = config.scheduler.sync_schedule_cron
    
    try:
        parts = sync_schedule_cron.split()
        
        if len(parts) == 5:
            minute, hour, day, month, day_of_week = parts
            
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week
            )
            
            logger.info(f"Scheduler configured with cron for protocol sync: {sync_schedule_cron}")
            return trigger
        else:
            # Если формат неверный, используем интервал (каждый день)
            logger.warning(f"Invalid cron format for sync: {sync_schedule_cron}. Using default daily schedule")
            return IntervalTrigger(days=1)
    
    except Exception as e:
        logger.error(f"Error configuring scheduler for protocol sync: {e}")
        logger.info("Using default daily interval for protocol sync")
        return IntervalTrigger(days=1)
