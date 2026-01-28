"""
Обработчики для Sync Manager.
"""

import logging
import traceback
from datetime import datetime
from typing import Tuple, Optional

from docreciv.sync_db.models import SyncRequest
from docreciv.webui.services.ui_service import get_ui_service

logger = logging.getLogger(__name__)


def sync_manager_start_sync(
    mode: str,
    from_date: Optional[datetime],
    to_date: Optional[datetime],
    batch_size: Optional[int],
    dry_run: bool,
    write_mode: str
) -> Tuple[str, str, str]:
    """
    Запустить синхронизацию через SyncManagerService.
    
    Returns:
        Кортеж (status_text, info_text, run_id)
    """
    try:
        ui_service = get_ui_service()
        sync_manager_service = ui_service.get_sync_manager_service()
        
        if sync_manager_service is None:
            return "❌ SyncManagerService не доступен", "", ""
        
        # Парсинг дат - поддержка как datetime объектов, так и строк
        from_dt = None
        to_dt = None
        
        if from_date:
            if isinstance(from_date, datetime):
                from_dt = from_date
            elif hasattr(from_date, 'strftime'):
                # Объект с методом strftime (например, из Gradio DateTime)
                from_dt = from_date
            elif isinstance(from_date, str) and from_date.strip():
                try:
                    # Пробуем разные форматы
                    if len(from_date) == 10:  # YYYY-MM-DD
                        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
                    elif 'T' in from_date or ' ' in from_date:  # ISO format или с временем
                        from_dt = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
                    else:
                        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
                except (ValueError, TypeError) as e:
                    return f"❌ Неверный формат from_date: {from_date}. Ошибка: {e}", "", ""
            else:
                # Пустая строка или None - это нормально для некоторых режимов
                pass
        
        if to_date:
            if isinstance(to_date, datetime):
                to_dt = to_date
            elif hasattr(to_date, 'strftime'):
                # Объект с методом strftime (например, из Gradio DateTime)
                to_dt = to_date
            elif isinstance(to_date, str) and to_date.strip():
                try:
                    # Пробуем разные форматы
                    if len(to_date) == 10:  # YYYY-MM-DD
                        to_dt = datetime.strptime(to_date, "%Y-%m-%d")
                    elif 'T' in to_date or ' ' in to_date:  # ISO format или с временем
                        to_dt = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
                    else:
                        to_dt = datetime.strptime(to_date, "%Y-%m-%d")
                except (ValueError, TypeError) as e:
                    return f"❌ Неверный формат to_date: {to_date}. Ошибка: {e}", "", ""
            else:
                # Пустая строка или None - это нормально для некоторых режимов
                pass
        
        # Валидация в зависимости от режима
        if mode in ["range", "backfill", "replay"]:
            if not from_date or not from_dt:
                return "❌ Для выбранного режима требуется начальная дата", "", ""
            if mode in ["range", "replay"] and (not to_date or not to_dt):
                return "❌ Для выбранного режима требуется конечная дата", "", ""
        
        # Для backfill: если to_date не указан, получаем из курсора или используем текущую дату
        if mode == "backfill" and not to_dt:
            cursor_state = sync_manager_service.get_cursor_state("protocols")
            if cursor_state and cursor_state.last_cursor_value:
                to_dt = cursor_state.last_cursor_value
            else:
                to_dt = datetime.utcnow()
        
        # Создать запрос
        # batch_size: None означает без ограничений, используем большое значение
        # SyncRequest требует положительное значение, поэтому используем 10000 как "без ограничений"
        batch_size_value = batch_size if batch_size and batch_size > 0 else 10000
        
        request = SyncRequest(
            collection="protocols",
            mode=mode,
            from_date=from_dt,
            to_date=to_dt,
            batch_size=batch_size_value,
            dry_run=dry_run,
            write_mode=write_mode,
            requested_by="webui"
        )
        
        # Запустить синхронизацию
        handle = sync_manager_service.start_sync(request)
        
        status_text = f"✅ Синхронизация запущена\nRun ID: {handle.run_id}\nРежим: {mode}"
        info_text = f"""Запрос синхронизации создан:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Run ID: {handle.run_id}
Режим: {mode}
Коллекция: protocols
Размер пакета: {batch_size}
Dry-run: {'Да' if dry_run else 'Нет'}
Режим записи: {write_mode}
Время создания: {handle.created_at.strftime('%Y-%m-%d %H:%M:%S')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        if from_dt and to_dt:
            info_text += f"Диапазон дат: {from_dt.date()} - {to_dt.date()}\n"
        
        return status_text, info_text, handle.run_id
        
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error starting sync: {error_details}")
        return f"❌ Ошибка: {e}", f"Детали:\n{error_details}", ""


def sync_manager_get_status(run_id: str) -> Tuple[str, str, float]:
    """
    Получить статус синхронизации.
    
    Returns:
        Кортеж (status_text, details_text, progress)
    """
    try:
        ui_service = get_ui_service()
        sync_manager_service = ui_service.get_sync_manager_service()
        
        if sync_manager_service is None:
            return "❌ SyncManagerService не доступен", "", 0.0
        
        if not run_id:
            return "⚠️ Укажите Run ID", "", 0.0
        
        status = sync_manager_service.get_status(run_id)
        
        if status is None:
            return "❌ Запуск не найден", "", 0.0
        
        status_text = f"""Статус: {status.status}
Прогресс: {status.progress:.1f}%
Обработано: {status.processed}
Вставлено: {status.inserted}
Пропущено: {status.skipped}
Ошибок: {status.errors}
"""
        
        if status.current_cursor:
            status_text += f"Текущий курсор: {status.current_cursor.strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        status_text += f"Сообщение: {status.message}"
        
        details_text = f"""Run ID: {run_id}
Статус: {status.status}
Прогресс: {status.progress:.1f}%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Обработано документов: {status.processed}
Вставлено новых: {status.inserted}
Пропущено (дубликаты): {status.skipped}
Ошибок: {status.errors}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        
        if status.current_cursor:
            details_text += f"Текущий курсор: {status.current_cursor.strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        return status_text, details_text, status.progress
        
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error getting sync status: {error_details}")
        return f"❌ Ошибка: {e}", f"Детали:\n{error_details}", 0.0


def sync_manager_cancel(run_id: str) -> str:
    """Отменить синхронизацию."""
    try:
        ui_service = get_ui_service()
        sync_manager_service = ui_service.get_sync_manager_service()
        
        if sync_manager_service is None:
            return "❌ SyncManagerService не доступен"
        
        if not run_id:
            return "⚠️ Укажите Run ID"
        
        success = sync_manager_service.cancel(run_id)
        
        if success:
            return f"✅ Синхронизация {run_id} отменена"
        else:
            return f"⚠️ Не удалось отменить синхронизацию {run_id} (возможно, уже завершена)"
        
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error cancelling sync: {error_details}")
        return f"❌ Ошибка: {e}"


def sync_manager_get_cursor_state() -> str:
    """Получить состояние курсора."""
    try:
        ui_service = get_ui_service()
        sync_manager_service = ui_service.get_sync_manager_service()
        
        if sync_manager_service is None:
            return "❌ SyncManagerService не доступен"
        
        cursor_state = sync_manager_service.get_cursor_state("protocols")
        
        if cursor_state is None:
            return "⚠️ Состояние курсора не найдено"
        
        return f"""Состояние курсора для коллекции 'protocols':
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Поле курсора: {cursor_state.cursor_field}
Последнее значение курсора: {cursor_state.last_cursor_value.strftime('%Y-%m-%d %H:%M:%S')}
Последний успешный запуск: {cursor_state.last_successful_run.strftime('%Y-%m-%d %H:%M:%S')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error getting cursor state: {error_details}")
        return f"❌ Ошибка: {e}"


def sync_manager_get_cursor_date() -> Optional[datetime]:
    """Получить дату последнего курсора для автоматической установки в UI."""
    try:
        ui_service = get_ui_service()
        sync_manager_service = ui_service.get_sync_manager_service()
        
        if sync_manager_service is None:
            return None
        
        cursor_state = sync_manager_service.get_cursor_state("protocols")
        
        if cursor_state is None or cursor_state.last_cursor_value is None:
            return None
        
        return cursor_state.last_cursor_value
        
    except Exception as e:
        logger.error(f"Error getting cursor date: {e}")
        return None


def sync_manager_get_recent_runs(limit: int = 10) -> str:
    """Получить список последних запусков."""
    try:
        ui_service = get_ui_service()
        sync_manager_service = ui_service.get_sync_manager_service()
        
        if sync_manager_service is None:
            return "❌ SyncManagerService не доступен"
        
        runs = sync_manager_service.get_recent_runs(limit=limit)
        
        if not runs:
            return "ℹ️ Нет запусков синхронизации"
        
        result = f"Последние {len(runs)} запусков синхронизации:\n"
        result += "=" * 80 + "\n"
        
        for run in runs:
            result += f"\nRun ID: {run.get('run_id', 'N/A')}\n"
            result += f"  Режим: {run.get('mode', 'N/A')}\n"
            result += f"  Статус: {run.get('status', 'N/A')}\n"
            result += f"  Создан: {run.get('created_at', 'N/A')}\n"
            
            stats = run.get('stats', {})
            if stats:
                result += f"  Обработано: {stats.get('processed', 0)}\n"
                result += f"  Вставлено: {stats.get('inserted', 0)}\n"
                result += f"  Ошибок: {stats.get('errors', 0)}\n"
            
            result += "-" * 80 + "\n"
        
        return result
        
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Error getting recent runs: {error_details}")
        return f"❌ Ошибка: {e}"

