"""
Обработчики для загрузки протоколов.
"""

import logging
import traceback
from datetime import datetime, timedelta
from typing import Tuple, Optional

from docreciv.downloader.models import DownloadRequest
from docreciv.webui.services.ui_service import get_ui_service
from docreciv.webui.utils.metrics_visualization import (
    create_download_progress_chart_figure,
    figure_to_pil_image
)

logger = logging.getLogger(__name__)


def download_protocols_handler(date_str: str = "", limit: int = 0) -> Tuple[str, str, str, any]:
    """
    Обработчик загрузки протоколов с детальными метриками.
    
    Args:
        date_str: Дата в формате YYYY-MM-DD (опционально)
        limit: Лимит протоколов (0 = без лимита)
        
    Returns:
        Кортеж (status_text, metrics_text, errors_text, chart_image)
    """
    try:
        ui_service = get_ui_service()
        downloader_service = ui_service.get_downloader_service()
        
        if downloader_service is None:
            return "❌ Downloader service not available", "", "", None
        
        # Если указана дата, фильтруем по дате
        target_date = None
        if date_str:
            try:
                target_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                pass
        
        # Выполняем загрузку
        result = downloader_service.process_pending_protocols(
            limit=limit if limit > 0 else None,
            target_date=target_date
        )
        
        # Форматируем детальные метрики
        metrics_text = f"""📊 Детальные метрики загрузки:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⏱️  Время выполнения: {result.duration:.2f} секунд
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 Основные показатели:
  • Обработано протоколов: {result.processed}
  • Успешно загружено: {result.downloaded}
  • Неудачных загрузок: {result.failed}
  
📊 Статистика:
  • Успешность: {(result.downloaded / result.processed * 100) if result.processed > 0 else 0:.1f}%
  • Скорость загрузки: {result.downloaded / result.duration if result.duration > 0 else 0:.2f} файлов/сек
"""
        
        # Добавляем статистику из result.statistics если есть
        if result.statistics:
            stats = result.statistics
            if "total_size" in stats:
                total_size_mb = stats["total_size"] / (1024 * 1024)
                metrics_text += f"  • Общий размер: {total_size_mb:.2f} MB\n"
            if "avg_file_size" in stats:
                avg_size_kb = stats["avg_file_size"] / 1024
                metrics_text += f"  • Средний размер файла: {avg_size_kb:.2f} KB\n"
        
        # Формируем информацию об ошибках
        errors_text = ""
        if result.errors and len(result.errors) > 0:
            errors_text = f"\n⚠️  Ошибки ({len(result.errors)}):\n"
            for i, error in enumerate(result.errors[:10], 1):
                errors_text += f"  {i}. {error}\n"
            if len(result.errors) > 10:
                errors_text += f"  ... и еще {len(result.errors) - 10} ошибок\n"
        
        # Создаем график прогресса
        chart_data = {
            "processed": result.processed,
            "downloaded": result.downloaded,
            "failed": result.failed
        }
        
        # Создаем график (возвращает Figure)
        chart_fig = create_download_progress_chart_figure(chart_data)
        
        # Конвертируем Figure в PIL Image
        chart_image = figure_to_pil_image(chart_fig) if chart_fig else None
        
        # Общий статус
        if result.status == "success":
            status_text = f"✅ Загрузка завершена успешно"
        else:
            status_text = f"❌ Загрузка завершена с ошибками: {result.message}"
        
        return status_text, metrics_text, errors_text, chart_image
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error in download_protocols_handler: {error_details}")
        return f"❌ Error: {e}", "", f"Детали ошибки:\n{error_details}", None


def download_protocols_advanced_handler(
    filter_type: str,
    from_date: Optional[datetime],
    to_date: Optional[datetime],
    max_units: int,
    max_urls_per_unit: int,
    force_reload: bool,
    skip_existing: bool,
    dry_run: bool
) -> Tuple[str, str, str, any]:
    """
    Расширенный обработчик загрузки протоколов с поддержкой всех опций фильтрации.
    
    Args:
        filter_type: Тип фильтра ("all", "last_day", "last_week", "last_month", "since_last", "custom")
        from_date: Начальная дата (для custom)
        to_date: Конечная дата (для custom)
        max_units: Максимальное количество UNIT (0 = без лимита)
        max_urls_per_unit: Максимальное количество URL на UNIT
        force_reload: Принудительная повторная загрузка
        skip_existing: Пропускать существующие UNIT
        dry_run: Тестовый режим
        
    Returns:
        Кортеж (status_text, metrics_text, errors_text, chart_image)
    """
    try:
        ui_service = get_ui_service()
        downloader_service = ui_service.get_downloader_service()
        
        if downloader_service is None:
            return "❌ Downloader service not available", "", "", None
        
        # Определяем диапазон дат в зависимости от типа фильтра
        request_from_date = None
        request_to_date = None
        now = datetime.utcnow()
        
        if filter_type == "last_day":
            request_from_date = now - timedelta(days=1)
            request_to_date = now
        elif filter_type == "last_week":
            request_from_date = now - timedelta(days=7)
            request_to_date = now
        elif filter_type == "last_month":
            request_from_date = now - timedelta(days=30)
            request_to_date = now
        elif filter_type == "since_last":
            # Получаем время последней загрузки
            last_download = downloader_service.get_last_download_timestamp()
            if last_download:
                request_from_date = last_download
                request_to_date = now
            else:
                # Если не найдено, используем последний день
                request_from_date = now - timedelta(days=1)
                request_to_date = now
                logger.warning("Last download timestamp not found, using last day")
        elif filter_type == "custom":
            # Преобразовать datetime если нужно
            if from_date:
                if isinstance(from_date, datetime):
                    request_from_date = from_date
                elif hasattr(from_date, 'strftime'):
                    request_from_date = from_date
                else:
                    try:
                        request_from_date = datetime.fromisoformat(str(from_date))
                    except (ValueError, TypeError):
                        request_from_date = None
            else:
                request_from_date = None
                
            if to_date:
                if isinstance(to_date, datetime):
                    request_to_date = to_date
                elif hasattr(to_date, 'strftime'):
                    request_to_date = to_date
                else:
                    try:
                        request_to_date = datetime.fromisoformat(str(to_date))
                    except (ValueError, TypeError):
                        request_to_date = None
            else:
                request_to_date = None
        # filter_type == "all" - оставляем None, None
        
        # Создаем DownloadRequest
        request = DownloadRequest(
            from_date=request_from_date,
            to_date=request_to_date,
            max_units_per_run=max_units if max_units > 0 else 0,
            max_urls_per_unit=max_urls_per_unit,
            dry_run=dry_run,
            force_reload=force_reload,
            skip_existing=skip_existing,
            requested_by="webui"
        )
        
        # Выполняем загрузку
        result = downloader_service.process_download_request(request)
        
        # Форматируем детальные метрики
        filter_info = ""
        date_range_info = ""
        if filter_type == "all":
            filter_info = "Все ожидающие протоколы"
        elif filter_type == "last_day":
            filter_info = "Последний день"
            date_range_info = f"({request_from_date.strftime('%Y-%m-%d %H:%M') if request_from_date else 'N/A'} - {request_to_date.strftime('%Y-%m-%d %H:%M') if request_to_date else 'N/A'})"
        elif filter_type == "last_week":
            filter_info = "Последняя неделя"
            date_range_info = f"({request_from_date.strftime('%Y-%m-%d') if request_from_date else 'N/A'} - {request_to_date.strftime('%Y-%m-%d') if request_to_date else 'N/A'})"
        elif filter_type == "last_month":
            filter_info = "Последний месяц"
            date_range_info = f"({request_from_date.strftime('%Y-%m-%d') if request_from_date else 'N/A'} - {request_to_date.strftime('%Y-%m-%d') if request_to_date else 'N/A'})"
        elif filter_type == "since_last":
            filter_info = f"С последней загрузки"
            date_range_info = f"({request_from_date.strftime('%Y-%m-%d %H:%M') if request_from_date else 'N/A'} - {request_to_date.strftime('%Y-%m-%d %H:%M') if request_to_date else 'N/A'})"
        elif filter_type == "custom":
            filter_info = "Произвольный период"
            date_range_info = f"({request_from_date.strftime('%Y-%m-%d') if request_from_date else 'N/A'} - {request_to_date.strftime('%Y-%m-%d') if request_to_date else 'N/A'})"
        
        metrics_text = f"""📊 Детальные метрики загрузки:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📅 Фильтр: {filter_info} {date_range_info}
⏱️  Время выполнения: {result.duration:.2f} секунд
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📈 Основные показатели:
  • Обработано протоколов: {result.processed}
  • Успешно загружено: {result.downloaded}
  • Неудачных загрузок: {result.failed}
  • Пропущено (существующие): {result.statistics.get('skipped_units', 0) if result.statistics else 0}
  
📊 Статистика:
  • Успешность: {(result.downloaded / result.processed * 100) if result.processed > 0 else 0:.1f}%
  • Скорость загрузки: {result.downloaded / result.duration if result.duration > 0 else 0:.2f} файлов/сек
"""
        
        # Добавляем предупреждения, если протоколы не найдены
        if result.processed == 0 and result.warnings:
            metrics_text += f"\n⚠️  {result.message}\n"
        
        # Добавляем статистику из result.statistics если есть
        if result.statistics:
            stats = result.statistics
            if "total_size" in stats:
                total_size_mb = stats["total_size"] / (1024 * 1024)
                metrics_text += f"  • Общий размер: {total_size_mb:.2f} MB\n"
            if "avg_file_size" in stats:
                avg_size_kb = stats["avg_file_size"] / 1024
                metrics_text += f"  • Средний размер файла: {avg_size_kb:.2f} KB\n"
            
            # Добавляем диагностику БД, если она есть
            if "total_protocols" in stats:
                metrics_text += (f"\n📊 Статистика БД:\n"
                                 f"  • Всего протоколов: {stats.get('total_protocols', 0)}\n"
                                 f"  • Ожидающих загрузки: {stats.get('pending_protocols', 0)}\n"
                                 f"  • Уже загружено: {stats.get('downloaded_protocols', 0)}\n")
            
            # Добавляем детальный отчет о статусе загрузки на основе файловой системы
            if "download_report" in stats:
                download_report = stats["download_report"]
                summary = download_report.get("summary", {})
                by_date = download_report.get("by_date", {})
                
                metrics_text += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                metrics_text += f"📁 Детальный отчет о статусе загрузки:\n"
                metrics_text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                metrics_text += f"📊 Сводка по диапазону дат:\n"
                metrics_text += f"  • Всего записей в БД: {summary.get('total_in_db', 0)}\n"
                metrics_text += f"  • Уже загружено UNIT: {summary.get('downloaded', 0)}\n"
                metrics_text += f"  • Ожидает загрузки: {summary.get('pending', 0)}\n"
                metrics_text += f"  • Обработано дат: {summary.get('dates_count', 0)}\n"
                
                # Показываем разбивку по датам (первые 10 дат)
                if by_date:
                    metrics_text += f"\n📅 Разбивка по датам (первые 10):\n"
                    for date_str, date_info in list(by_date.items())[:10]:
                        metrics_text += f"  • {date_str}:\n"
                        metrics_text += f"    - В БД: {date_info.get('total_in_db', 0)}\n"
                        metrics_text += f"    - Загружено UNIT: {date_info.get('downloaded_units', 0)}\n"
                        metrics_text += f"    - Ожидает: {date_info.get('pending_units', 0)}\n"
                        metrics_text += f"    - Файлов: {date_info.get('total_files', 0)}\n"
                    
                    if len(by_date) > 10:
                        metrics_text += f"  ... и еще {len(by_date) - 10} дат\n"
                
                # Добавляем рекомендации
                recommendations = summary.get("recommendations", [])
                if recommendations:
                    metrics_text += f"\n💡 Рекомендации:\n"
                    for rec in recommendations[:5]:
                        metrics_text += f"  • {rec}\n"
            
            # Добавляем альтернативную статистику, если нет детального отчета
            elif "total_in_db" in stats or "already_downloaded" in stats:
                metrics_text += f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                metrics_text += f"📁 Статус загрузки на основе файловой системы:\n"
                metrics_text += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                if "total_in_db" in stats:
                    metrics_text += f"  • Всего записей в БД: {stats.get('total_in_db', 0)}\n"
                if "already_downloaded" in stats:
                    metrics_text += f"  • Уже загружено UNIT: {stats.get('already_downloaded', 0)}\n"
                if "pending_to_download" in stats:
                    metrics_text += f"  • Ожидает загрузки: {stats.get('pending_to_download', 0)}\n"
        
        # Формируем информацию об ошибках
        errors_text = ""
        if result.errors and len(result.errors) > 0:
            errors_text = f"\n⚠️  Ошибки ({len(result.errors)}):\n"
            for i, error in enumerate(result.errors[:10], 1):
                errors_text += f"  {i}. {error}\n"
            if len(result.errors) > 10:
                errors_text += f"  ... и еще {len(result.errors) - 10} ошибок\n"
        
        # Создаем график прогресса
        chart_data = {
            "processed": result.processed,
            "downloaded": result.downloaded,
            "failed": result.failed
        }
        
        # Создаем график (возвращает Figure)
        chart_fig = create_download_progress_chart_figure(chart_data)
        
        # Конвертируем Figure в PIL Image
        chart_image = figure_to_pil_image(chart_fig) if chart_fig else None
        
        # Общий статус
        if result.status == "success":
            status_text = f"✅ Загрузка завершена успешно"
        elif result.status == "partial":
            status_text = f"⚠️ Загрузка завершена частично: {result.message}"
        else:
            status_text = f"❌ Загрузка завершена с ошибками: {result.message}"
        
        return status_text, metrics_text, errors_text, chart_image
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error in download_protocols_advanced_handler: {error_details}")
        return f"❌ Error: {e}", "", f"Детали ошибки:\n{error_details}", None

