"""
Утилита для агрегации метрик синхронизации по различным периодам.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


def aggregate_by_days_of_week(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Агрегировать данные по дням недели.
    
    Args:
        data: Список записей с полями date (YYYY-MM-DD) и метриками
        
    Returns:
        Словарь {day_name: {metrics}}
    """
    day_names = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]
    aggregated = defaultdict(lambda: {
        "scanned": 0,
        "inserted": 0,
        "skipped": 0,
        "errors": 0,
        "duration": 0,
        "count": 0
    })
    
    for record in data:
        date_str = record.get("date", "")
        if not date_str:
            continue
        
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            day_of_week = date_obj.weekday()  # 0 = Monday, 6 = Sunday
            day_name = day_names[day_of_week]
            
            aggregated[day_name]["scanned"] += record.get("total_scanned", 0)
            aggregated[day_name]["inserted"] += record.get("total_inserted", 0)
            aggregated[day_name]["skipped"] += record.get("total_skipped", 0)
            aggregated[day_name]["errors"] += record.get("total_errors", 0)
            aggregated[day_name]["duration"] += record.get("duration_seconds", 0)
            aggregated[day_name]["count"] += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            continue
    
    # Вычисляем средние значения
    result = {}
    for day_name in day_names:
        if day_name in aggregated:
            stats = aggregated[day_name]
            result[day_name] = {
                "total_scanned": stats["scanned"],
                "total_inserted": stats["inserted"],
                "total_skipped": stats["skipped"],
                "total_errors": stats["errors"],
                "total_duration": stats["duration"],
                "avg_scanned": stats["scanned"] / stats["count"] if stats["count"] > 0 else 0,
                "avg_inserted": stats["inserted"] / stats["count"] if stats["count"] > 0 else 0,
                "sessions_count": stats["count"]
            }
        else:
            result[day_name] = {
                "total_scanned": 0,
                "total_inserted": 0,
                "total_skipped": 0,
                "total_errors": 0,
                "total_duration": 0,
                "avg_scanned": 0,
                "avg_inserted": 0,
                "sessions_count": 0
            }
    
    return result


def aggregate_by_days_of_month(data: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """
    Агрегировать данные по дням месяца (1-31).
    
    Args:
        data: Список записей с полями date (YYYY-MM-DD) и метриками
        
    Returns:
        Словарь {day_of_month: {metrics}}
    """
    aggregated = defaultdict(lambda: {
        "scanned": 0,
        "inserted": 0,
        "skipped": 0,
        "errors": 0,
        "duration": 0,
        "count": 0
    })
    
    for record in data:
        date_str = record.get("date", "")
        if not date_str:
            continue
        
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            day_of_month = date_obj.day
            
            aggregated[day_of_month]["scanned"] += record.get("total_scanned", 0)
            aggregated[day_of_month]["inserted"] += record.get("total_inserted", 0)
            aggregated[day_of_month]["skipped"] += record.get("total_skipped", 0)
            aggregated[day_of_month]["errors"] += record.get("total_errors", 0)
            aggregated[day_of_month]["duration"] += record.get("duration_seconds", 0)
            aggregated[day_of_month]["count"] += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            continue
    
    # Вычисляем средние значения
    result = {}
    for day in range(1, 32):
        if day in aggregated:
            stats = aggregated[day]
            result[day] = {
                "total_scanned": stats["scanned"],
                "total_inserted": stats["inserted"],
                "total_skipped": stats["skipped"],
                "total_errors": stats["errors"],
                "total_duration": stats["duration"],
                "avg_scanned": stats["scanned"] / stats["count"] if stats["count"] > 0 else 0,
                "avg_inserted": stats["inserted"] / stats["count"] if stats["count"] > 0 else 0,
                "sessions_count": stats["count"]
            }
        else:
            result[day] = {
                "total_scanned": 0,
                "total_inserted": 0,
                "total_skipped": 0,
                "total_errors": 0,
                "total_duration": 0,
                "avg_scanned": 0,
                "avg_inserted": 0,
                "sessions_count": 0
            }
    
    return result


def aggregate_by_days_simple(data: List[Dict[str, Any]], days: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    """
    Агрегировать данные по дням (простая агрегация, аналог get_sync_trends).
    
    Args:
        data: Список записей с полями date (YYYY-MM-DD) и метриками
        days: Количество дней для фильтрации (None = все данные)
        
    Returns:
        Словарь {date: {metrics}} с усредненными значениями
    """
    from collections import defaultdict
    
    if days:
        cutoff_date = datetime.utcnow() - timedelta(days=days)
    else:
        cutoff_date = None
    
    aggregated = defaultdict(lambda: {
        "scanned": 0,
        "inserted": 0,
        "skipped": 0,
        "errors": 0,
        "duration": 0,
        "count": 0
    })
    
    for record in data:
        date_str = record.get("date", "")
        if not date_str:
            continue
        
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            if cutoff_date and date_obj < cutoff_date:
                continue
            
            aggregated[date_str]["scanned"] += record.get("total_scanned", 0)
            aggregated[date_str]["inserted"] += record.get("total_inserted", 0)
            aggregated[date_str]["skipped"] += record.get("total_skipped", 0)
            aggregated[date_str]["errors"] += record.get("total_errors", 0)
            aggregated[date_str]["duration"] += record.get("duration_seconds", 0)
            aggregated[date_str]["count"] += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            continue
    
    # Вычисляем средние значения (формат как в get_sync_trends)
    result = {}
    for date_str, stats in sorted(aggregated.items()):
        result[date_str] = {
            "avg_scanned": stats["scanned"] / stats["count"] if stats["count"] > 0 else 0,
            "avg_inserted": stats["inserted"] / stats["count"] if stats["count"] > 0 else 0,
            "avg_skipped": stats["skipped"] / stats["count"] if stats["count"] > 0 else 0,
            "avg_errors": stats["errors"] / stats["count"] if stats["count"] > 0 else 0,
            "avg_duration": stats["duration"] / stats["count"] if stats["count"] > 0 else 0,
            "sessions": stats["count"]
        }
    
    return result


def aggregate_by_days_last_3_months(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Агрегировать данные по дням за последние 3 месяца.
    
    Args:
        data: Список записей с полями date (YYYY-MM-DD) и метриками
        
    Returns:
        Словарь {date: {metrics}}
    """
    three_months_ago = datetime.utcnow() - timedelta(days=90)
    aggregated = {}
    
    for record in data:
        date_str = record.get("date", "")
        if not date_str:
            continue
        
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            if date_obj < three_months_ago:
                continue
            
            if date_str not in aggregated:
                aggregated[date_str] = {
                    "scanned": 0,
                    "inserted": 0,
                    "skipped": 0,
                    "errors": 0,
                    "duration": 0,
                    "count": 0
                }
            
            aggregated[date_str]["scanned"] += record.get("total_scanned", 0)
            aggregated[date_str]["inserted"] += record.get("total_inserted", 0)
            aggregated[date_str]["skipped"] += record.get("total_skipped", 0)
            aggregated[date_str]["errors"] += record.get("total_errors", 0)
            aggregated[date_str]["duration"] += record.get("duration_seconds", 0)
            aggregated[date_str]["count"] += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            continue
    
    # Вычисляем средние значения
    result = {}
    for date_str, stats in sorted(aggregated.items()):
        result[date_str] = {
            "total_scanned": stats["scanned"],
            "total_inserted": stats["inserted"],
            "total_skipped": stats["skipped"],
            "total_errors": stats["errors"],
            "total_duration": stats["duration"],
            "avg_scanned": stats["scanned"] / stats["count"] if stats["count"] > 0 else 0,
            "avg_inserted": stats["inserted"] / stats["count"] if stats["count"] > 0 else 0,
            "sessions_count": stats["count"]
        }
    
    return result


def aggregate_by_weeks_last_3_months(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Агрегировать данные по неделям за последние 3 месяца.
    
    Args:
        data: Список записей с полями date (YYYY-MM-DD) и метриками
        
    Returns:
        Словарь {week_label: {metrics}} где week_label = "YYYY-WW"
    """
    three_months_ago = datetime.utcnow() - timedelta(days=90)
    aggregated = defaultdict(lambda: {
        "scanned": 0,
        "inserted": 0,
        "skipped": 0,
        "errors": 0,
        "duration": 0,
        "count": 0
    })
    
    for record in data:
        date_str = record.get("date", "")
        if not date_str:
            continue
        
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            if date_obj < three_months_ago:
                continue
            
            # Получаем номер недели в году
            year, week, _ = date_obj.isocalendar()
            week_label = f"{year}-W{week:02d}"
            
            aggregated[week_label]["scanned"] += record.get("total_scanned", 0)
            aggregated[week_label]["inserted"] += record.get("total_inserted", 0)
            aggregated[week_label]["skipped"] += record.get("total_skipped", 0)
            aggregated[week_label]["errors"] += record.get("total_errors", 0)
            aggregated[week_label]["duration"] += record.get("duration_seconds", 0)
            aggregated[week_label]["count"] += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            continue
    
    # Вычисляем средние значения
    result = {}
    for week_label in sorted(aggregated.keys()):
        stats = aggregated[week_label]
        result[week_label] = {
            "total_scanned": stats["scanned"],
            "total_inserted": stats["inserted"],
            "total_skipped": stats["skipped"],
            "total_errors": stats["errors"],
            "total_duration": stats["duration"],
            "avg_scanned": stats["scanned"] / stats["count"] if stats["count"] > 0 else 0,
            "avg_inserted": stats["inserted"] / stats["count"] if stats["count"] > 0 else 0,
            "sessions_count": stats["count"]
        }
    
    return result


def aggregate_by_weeks_per_year(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Агрегировать данные по неделям за год.
    
    Args:
        data: Список записей с полями date (YYYY-MM-DD) и метриками
        
    Returns:
        Словарь {week_label: {metrics}} где week_label = "YYYY-WW"
    """
    aggregated = defaultdict(lambda: {
        "scanned": 0,
        "inserted": 0,
        "skipped": 0,
        "errors": 0,
        "duration": 0,
        "count": 0
    })
    
    for record in data:
        date_str = record.get("date", "")
        if not date_str:
            continue
        
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            
            # Получаем номер недели в году
            year, week, _ = date_obj.isocalendar()
            week_label = f"{year}-W{week:02d}"
            
            aggregated[week_label]["scanned"] += record.get("total_scanned", 0)
            aggregated[week_label]["inserted"] += record.get("total_inserted", 0)
            aggregated[week_label]["skipped"] += record.get("total_skipped", 0)
            aggregated[week_label]["errors"] += record.get("total_errors", 0)
            aggregated[week_label]["duration"] += record.get("duration_seconds", 0)
            aggregated[week_label]["count"] += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            continue
    
    # Вычисляем средние значения
    result = {}
    for week_label in sorted(aggregated.keys()):
        stats = aggregated[week_label]
        result[week_label] = {
            "total_scanned": stats["scanned"],
            "total_inserted": stats["inserted"],
            "total_skipped": stats["skipped"],
            "total_errors": stats["errors"],
            "total_duration": stats["duration"],
            "avg_scanned": stats["scanned"] / stats["count"] if stats["count"] > 0 else 0,
            "avg_inserted": stats["inserted"] / stats["count"] if stats["count"] > 0 else 0,
            "sessions_count": stats["count"]
        }
    
    return result


def aggregate_by_months_per_year(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Агрегировать данные по месяцам за год.
    
    Args:
        data: Список записей с полями date (YYYY-MM-DD) и метриками
        
    Returns:
        Словарь {month_label: {metrics}} где month_label = "YYYY-MM"
    """
    aggregated = defaultdict(lambda: {
        "scanned": 0,
        "inserted": 0,
        "skipped": 0,
        "errors": 0,
        "duration": 0,
        "count": 0
    })
    
    for record in data:
        date_str = record.get("date", "")
        if not date_str:
            continue
        
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            month_label = date_obj.strftime("%Y-%m")
            
            aggregated[month_label]["scanned"] += record.get("total_scanned", 0)
            aggregated[month_label]["inserted"] += record.get("total_inserted", 0)
            aggregated[month_label]["skipped"] += record.get("total_skipped", 0)
            aggregated[month_label]["errors"] += record.get("total_errors", 0)
            aggregated[month_label]["duration"] += record.get("duration_seconds", 0)
            aggregated[month_label]["count"] += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parsing date {date_str}: {e}")
            continue
    
    # Вычисляем средние значения
    result = {}
    for month_label in sorted(aggregated.keys()):
        stats = aggregated[month_label]
        result[month_label] = {
            "total_scanned": stats["scanned"],
            "total_inserted": stats["inserted"],
            "total_skipped": stats["skipped"],
            "total_errors": stats["errors"],
            "total_duration": stats["duration"],
            "avg_scanned": stats["scanned"] / stats["count"] if stats["count"] > 0 else 0,
            "avg_inserted": stats["inserted"] / stats["count"] if stats["count"] > 0 else 0,
            "sessions_count": stats["count"]
        }
    
    return result

