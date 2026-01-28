"""
Утилита для визуализации метрик синхронизации.
Создает графики и таблицы для отображения статистики.
Объединенный модуль, заменяющий charts.py.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import io
import base64

logger = logging.getLogger(__name__)

try:
    import matplotlib
    matplotlib.use('Agg')  # Используем non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.figure import Figure
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    logger.warning("matplotlib not available, charts will be disabled")

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("pandas not available, tables will be limited")

from docreciv.sync_db.analytics import SyncAnalytics
from docreciv.webui.utils.metrics_aggregator import (
    aggregate_by_days_of_week,
    aggregate_by_days_of_month,
    aggregate_by_days_last_3_months,
    aggregate_by_weeks_last_3_months,
    aggregate_by_weeks_per_year,
    aggregate_by_months_per_year
)

logger = logging.getLogger(__name__)


def get_sync_metrics_data(days: int = 365) -> List[Dict[str, Any]]:
    """
    Получить данные метрик синхронизации из SyncAnalytics.
    
    Args:
        days: Количество дней истории для получения
        
    Returns:
        Список записей с метриками
    """
    try:
        analytics = SyncAnalytics()
        data = analytics.get_historical_sync_data(days=days)
        analytics.close()
        return data
    except Exception as e:
        logger.error(f"Error getting sync metrics data: {e}")
        return []


def create_metrics_chart(
    aggregated_data: Dict[str, Dict[str, Any]],
    filter_type: str,
    metric_type: str = "inserted"
) -> Optional[Figure]:
    """
    Создать график метрик синхронизации.
    
    Args:
        aggregated_data: Агрегированные данные по периодам
        filter_type: Тип фильтра (для подписи графика)
        metric_type: Тип метрики для отображения ("inserted", "scanned", "errors")
        
    Returns:
        Figure объект matplotlib или None
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Подготовка данных
        labels = []
        values = []
        
        for key in sorted(aggregated_data.keys()):
            labels.append(str(key))
            values.append(aggregated_data[key].get(f"total_{metric_type}", 0))
        
        # Создание графика
        if filter_type in ["По дням недели", "По дням месяца"]:
            ax.bar(labels, values, color='steelblue', alpha=0.7)
        else:
            ax.plot(labels, values, marker='o', linewidth=2, markersize=6, color='steelblue')
            ax.fill_between(labels, values, alpha=0.3, color='steelblue')
        
        # Настройка осей
        ax.set_xlabel('Период', fontsize=10)
        metric_labels = {
            "inserted": "Вставлено документов",
            "scanned": "Просмотрено документов",
            "errors": "Ошибок"
        }
        ax.set_ylabel(metric_labels.get(metric_type, metric_type), fontsize=10)
        ax.set_title(f'Метрики синхронизации: {metric_labels.get(metric_type, metric_type)} ({filter_type})', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        # Поворот подписей на оси X для длинных меток
        if len(labels) > 10:
            plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        logger.error(f"Error creating metrics chart: {e}")
        return None


def create_metrics_table(
    aggregated_data: Dict[str, Dict[str, Any]],
    filter_type: str
) -> Tuple[List[List[str]], List[str]]:
    """
    Создать таблицу метрик синхронизации.
    
    Args:
        aggregated_data: Агрегированные данные по периодам
        filter_type: Тип фильтра (для заголовка)
        
    Returns:
        Кортеж (данные таблицы, заголовки)
    """
    try:
        headers = ["Период", "Просмотрено", "Вставлено", "Пропущено", "Ошибок", "Сессий", "Среднее вставлено"]
        rows = []
        
        for key in sorted(aggregated_data.keys()):
            stats = aggregated_data[key]
            rows.append([
                str(key),
                str(stats.get("total_scanned", 0)),
                str(stats.get("total_inserted", 0)),
                str(stats.get("total_skipped", 0)),
                str(stats.get("total_errors", 0)),
                str(stats.get("sessions_count", 0)),
                f"{stats.get('avg_inserted', 0):.2f}"
            ])
        
        return rows, headers
        
    except Exception as e:
        logger.error(f"Error creating metrics table: {e}")
        return [], []


def get_metrics_visualization(
    filter_type: str,
    days: int = 365
) -> Tuple[Optional[Figure], List[List[str]], List[str]]:
    """
    Получить визуализацию метрик синхронизации (график + таблица).
    
    Args:
        filter_type: Тип фильтра агрегации
        days: Количество дней истории
        
    Returns:
        Кортеж (график Figure, данные таблицы, заголовки таблицы)
    """
    try:
        # Получаем данные
        raw_data = get_sync_metrics_data(days)
        
        if not raw_data:
            logger.warning("No sync metrics data available")
            return None, [], []
        
        # Агрегируем данные в зависимости от типа фильтра
        if filter_type == "По дням недели":
            aggregated = aggregate_by_days_of_week(raw_data)
        elif filter_type == "По дням месяца":
            aggregated = aggregate_by_days_of_month(raw_data)
        elif filter_type == "По дням за 3 месяца":
            aggregated = aggregate_by_days_last_3_months(raw_data)
        elif filter_type == "По неделям за 3 месяца":
            aggregated = aggregate_by_weeks_last_3_months(raw_data)
        elif filter_type == "По неделям за год":
            aggregated = aggregate_by_weeks_per_year(raw_data)
        elif filter_type == "По месяцам за год":
            aggregated = aggregate_by_months_per_year(raw_data)
        else:
            # По умолчанию - по дням за 3 месяца
            aggregated = aggregate_by_days_last_3_months(raw_data)
        
        # Создаем график
        chart = create_metrics_chart(aggregated, filter_type, "inserted")
        
        # Создаем таблицу
        table_data, table_headers = create_metrics_table(aggregated, filter_type)
        
        return chart, table_data, table_headers
        
    except Exception as e:
        logger.error(f"Error getting metrics visualization: {e}")
        return None, [], []


def figure_to_image(fig: Optional[Figure]) -> Optional[bytes]:
    """
    Конвертировать matplotlib Figure в изображение (PNG bytes).
    
    Args:
        fig: Figure объект matplotlib
        
    Returns:
        PNG bytes или None
    """
    if not fig or not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        image_bytes = buf.read()
        buf.close()
        plt.close(fig)  # Освобождаем память
        return image_bytes
    except Exception as e:
        logger.error(f"Error converting figure to image: {e}")
        if fig:
            plt.close(fig)
        return None


def figure_to_base64(fig: Optional[Figure], dpi: int = 300) -> str:
    """
    Конвертировать matplotlib Figure в base64 строку.
    
    Args:
        fig: Figure объект matplotlib
        dpi: Разрешение изображения
        
    Returns:
        Base64 строка с префиксом data:image/png;base64,
    """
    if not fig or not MATPLOTLIB_AVAILABLE:
        return create_placeholder_chart_base64("Chart unavailable")
    
    try:
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=dpi, bbox_inches='tight')
        buf.seek(0)
        img_str = base64.b64encode(buf.getvalue()).decode()
        buf.close()
        plt.close(fig)
        return f"data:image/png;base64,{img_str}"
    except Exception as e:
        logger.error(f"Error converting figure to base64: {e}")
        if fig:
            plt.close(fig)
        return create_placeholder_chart_base64(f"Error: {e}")


def figure_to_pil_image(fig: Optional[Figure]) -> Optional[Any]:
    """
    Конвертировать matplotlib Figure в PIL Image.
    
    Args:
        fig: Figure объект matplotlib
        
    Returns:
        PIL Image или None
    """
    try:
        from PIL import Image
    except ImportError:
        logger.warning("PIL not available")
        return None
    
    if not fig or not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        image_bytes = figure_to_image(fig)
        if image_bytes:
            return Image.open(io.BytesIO(image_bytes))
        return None
    except Exception as e:
        logger.error(f"Error converting figure to PIL image: {e}")
        return None


# ========== Функции для создания графиков (возвращают Figure) ==========

def create_sync_trend_chart_figure(data: Dict[str, Any]) -> Optional[Figure]:
    """
    Создает график трендов синхронизации.
    
    Args:
        data: Данные трендов синхронизации
        
    Returns:
        Figure объект matplotlib или None
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        if not data or "daily_averages" not in data:
            return None
        
        daily_data = data["daily_averages"]
        dates = list(daily_data.keys())
        
        if not dates:
            return None
        
        # Сортируем даты
        dates.sort()
        
        # Извлекаем данные для графика
        scanned = [daily_data[date].get("avg_scanned", 0) for date in dates]
        inserted = [daily_data[date].get("avg_inserted", 0) for date in dates]
        errors = [daily_data[date].get("avg_errors", 0) for date in dates]
        
        # Создаем график
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Преобразуем даты в datetime объекты для оси X
        x_dates = [datetime.strptime(date, "%Y-%m-%d") for date in dates]
        
        ax.plot(x_dates, scanned, marker='o', label='Scanned', linewidth=2)
        ax.plot(x_dates, inserted, marker='s', label='Inserted', linewidth=2)
        ax.plot(x_dates, errors, marker='^', label='Errors', linewidth=2)
        
        ax.set_xlabel('Date')
        ax.set_ylabel('Count')
        ax.set_title('Sync Trends Over Time')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Поворачиваем подписи дат
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create sync trend chart: {e}")
        return None


def create_performance_chart_figure(data: Dict[str, Any]) -> Optional[Figure]:
    """
    Создает график производительности.
    
    Args:
        data: Данные производительности
        
    Returns:
        Figure объект matplotlib или None
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        if not data or "daily_averages" not in data:
            return None
        
        daily_data = data["daily_averages"]
        dates = list(daily_data.keys())
        
        if not dates:
            return None
        
        # Сортируем даты
        dates.sort()
        
        # Извлекаем данные для графика
        duration = [daily_data[date].get("avg_duration", 0) for date in dates]
        
        # Создаем график
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Преобразуем даты в datetime объекты для оси X
        x_dates = [datetime.strptime(date, "%Y-%m-%d") for date in dates]
        
        ax.bar(x_dates, duration, color='skyblue', alpha=0.7)
        ax.set_xlabel('Date')
        ax.set_ylabel('Duration (seconds)')
        ax.set_title('Average Sync Duration Over Time')
        
        # Поворачиваем подписи дат
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create performance chart: {e}")
        return None


def create_error_distribution_chart_figure(data: Dict[str, Any]) -> Optional[Figure]:
    """
    Создает круговую диаграмму распределения ошибок.
    
    Args:
        data: Данные об ошибках
        
    Returns:
        Figure объект matplotlib или None
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        # Заглушка для распределения ошибок
        labels = ['Network', 'Database', 'Parsing', 'Timeout', 'Other']
        sizes = [30, 25, 20, 15, 10]
        colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff99cc']
        
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        ax.axis('equal')
        ax.set_title('Error Distribution')
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create error distribution chart: {e}")
        return None


def create_sync_progress_chart_figure(data: Dict[str, Any], date_str: str = "") -> Optional[Figure]:
    """
    Создает график прогресса синхронизации.
    
    Args:
        data: Данные синхронизации (scanned, inserted, skipped, errors)
        date_str: Дата синхронизации
        
    Returns:
        Figure объект matplotlib или None
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        if not data:
            return None
        
        # Создаем график
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # График 1: Столбчатая диаграмма результатов
        categories = ['Отсканировано', 'Вставлено', 'Пропущено', 'Ошибки']
        values = [
            data.get("scanned", 0),
            data.get("inserted", 0),
            data.get("skipped", 0),
            data.get("errors", 0)
        ]
        colors = ['#3498db', '#2ecc71', '#f39c12', '#e74c3c']
        
        bars = ax1.bar(categories, values, color=colors, alpha=0.7, edgecolor='black', linewidth=1.2)
        ax1.set_ylabel('Количество', fontsize=12, fontweight='bold')
        ax1.set_title('Результаты синхронизации', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Добавляем значения на столбцы
        for bar, value in zip(bars, values):
            if value > 0:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(value)}',
                        ha='center', va='bottom', fontweight='bold')
        
        # График 2: Круговая диаграмма распределения
        labels = ['Вставлено', 'Пропущено', 'Ошибки']
        sizes = [
            data.get("inserted", 0),
            data.get("skipped", 0),
            data.get("errors", 0)
        ]
        # Убираем нулевые значения
        filtered_data = [(label, size) for label, size in zip(labels, sizes) if size > 0]
        if filtered_data:
            labels, sizes = zip(*filtered_data)
            colors_pie = ['#2ecc71', '#f39c12', '#e74c3c'][:len(labels)]
            
            ax2.pie(sizes, labels=labels, colors=colors_pie, autopct='%1.1f%%',
                   startangle=90, textprops={'fontsize': 11, 'fontweight': 'bold'})
            ax2.set_title('Распределение результатов', fontsize=14, fontweight='bold')
        else:
            ax2.text(0.5, 0.5, 'Нет данных', ha='center', va='center',
                    transform=ax2.transAxes, fontsize=14, color='gray')
            ax2.axis('off')
        
        if date_str:
            fig.suptitle(f'Синхронизация за {date_str}', fontsize=16, fontweight='bold', y=1.02)
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create sync progress chart: {e}")
        return None


def create_download_progress_chart_figure(data: Dict[str, Any]) -> Optional[Figure]:
    """
    Создает график прогресса загрузки.
    
    Args:
        data: Данные загрузки (processed, downloaded, failed)
        
    Returns:
        Figure объект matplotlib или None
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        if not data:
            return None
        
        # Создаем график
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # График 1: Столбчатая диаграмма результатов
        categories = ['Обработано', 'Загружено', 'Ошибки']
        values = [
            data.get("processed", 0),
            data.get("downloaded", 0),
            data.get("failed", 0)
        ]
        colors = ['#3498db', '#2ecc71', '#e74c3c']
        
        bars = ax1.bar(categories, values, color=colors, alpha=0.7, edgecolor='black', linewidth=1.2)
        ax1.set_ylabel('Количество', fontsize=12, fontweight='bold')
        ax1.set_title('Результаты загрузки', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Добавляем значения на столбцы
        for bar, value in zip(bars, values):
            if value > 0:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(value)}',
                        ha='center', va='bottom', fontweight='bold')
        
        # График 2: Круговая диаграмма успешности
        downloaded = data.get("downloaded", 0)
        failed = data.get("failed", 0)
        
        if downloaded + failed > 0:
            labels = ['Успешно', 'Ошибки']
            sizes = [downloaded, failed]
            colors_pie = ['#2ecc71', '#e74c3c']
            
            ax2.pie(sizes, labels=labels, colors=colors_pie, autopct='%1.1f%%',
                   startangle=90, textprops={'fontsize': 11, 'fontweight': 'bold'})
            ax2.set_title('Успешность загрузки', fontsize=14, fontweight='bold')
        else:
            ax2.text(0.5, 0.5, 'Нет данных', ha='center', va='center',
                    transform=ax2.transAxes, fontsize=14, color='gray')
            ax2.axis('off')
        
        fig.suptitle('Статистика загрузки протоколов', fontsize=16, fontweight='bold', y=1.02)
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create download progress chart: {e}")
        return None


def create_download_stats_chart_figure(data: List[Dict[str, Any]]) -> Optional[Figure]:
    """
    Создает график статистики загрузки.
    
    Args:
        data: Данные статистики загрузки
        
    Returns:
        Figure объект matplotlib или None
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        if not data:
            return None
        
        # Извлекаем данные для графика
        dates = [item.get("date", "") for item in data]
        downloaded = [item.get("downloaded", 0) for item in data]
        failed = [item.get("failed", 0) for item in data]
        
        # Создаем график
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Преобразуем даты в datetime объекты для оси X
        x_dates = [datetime.strptime(date, "%Y-%m-%d") for date in dates if date]
        
        if x_dates:
            ax.plot(x_dates, downloaded, marker='o', label='Downloaded', linewidth=2)
            ax.plot(x_dates, failed, marker='s', label='Failed', linewidth=2)
            
            ax.set_xlabel('Date')
            ax.set_ylabel('Count')
            ax.set_title('Download Statistics Over Time')
            ax.legend()
            ax.grid(True, alpha=0.3)
            
            # Поворачиваем подписи дат
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create download stats chart: {e}")
        return None


def create_placeholder_chart_figure(message: str) -> Optional[Figure]:
    """
    Создает placeholder график с сообщением.
    
    Args:
        message: Сообщение для отображения
        
    Returns:
        Figure объект matplotlib или None
    """
    if not MATPLOTLIB_AVAILABLE:
        return None
    
    try:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, message, ha='center', va='center', transform=ax.transAxes, 
                fontsize=14, color='gray')
        ax.axis('off')
        ax.set_title('Chart Unavailable')
        
        plt.tight_layout()
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create placeholder chart: {e}")
        return None


# ========== Функции для обратной совместимости (возвращают base64) ==========

def create_placeholder_chart_base64(message: str) -> str:
    """
    Создает placeholder график с сообщением (base64).
    
    Args:
        message: Сообщение для отображения
        
    Returns:
        str: Base64 encoded изображение графика
    """
    fig = create_placeholder_chart_figure(message)
    if fig:
        return figure_to_base64(fig, dpi=300)
    return ""


def create_sync_trend_chart(data: Dict[str, Any]) -> str:
    """
    Создает график трендов синхронизации (base64 для обратной совместимости).
    
    Args:
        data: Данные трендов синхронизации
        
    Returns:
        str: Base64 encoded изображение графика
    """
    fig = create_sync_trend_chart_figure(data)
    if fig:
        return figure_to_base64(fig, dpi=300)
    return create_placeholder_chart_base64("No sync data available")


def create_performance_chart(data: Dict[str, Any]) -> str:
    """
    Создает график производительности (base64 для обратной совместимости).
    
    Args:
        data: Данные производительности
        
    Returns:
        str: Base64 encoded изображение графика
    """
    fig = create_performance_chart_figure(data)
    if fig:
        return figure_to_base64(fig, dpi=300)
    return create_placeholder_chart_base64("No performance data available")


def create_error_distribution_chart(data: Dict[str, Any]) -> str:
    """
    Создает круговую диаграмму распределения ошибок (base64 для обратной совместимости).
    
    Args:
        data: Данные об ошибках
        
    Returns:
        str: Base64 encoded изображение графика
    """
    fig = create_error_distribution_chart_figure(data)
    if fig:
        return figure_to_base64(fig, dpi=300)
    return create_placeholder_chart_base64("Error: Failed to create chart")


def create_sync_progress_chart(data: Dict[str, Any], date_str: str = "") -> str:
    """
    Создает график прогресса синхронизации (base64 для обратной совместимости).
    
    Args:
        data: Данные синхронизации (scanned, inserted, skipped, errors)
        date_str: Дата синхронизации
        
    Returns:
        str: Base64 encoded изображение графика
    """
    fig = create_sync_progress_chart_figure(data, date_str)
    if fig:
        return figure_to_base64(fig, dpi=150)
    return create_placeholder_chart_base64("No sync data available")


def create_download_progress_chart(data: Dict[str, Any]) -> str:
    """
    Создает график прогресса загрузки (base64 для обратной совместимости).
    
    Args:
        data: Данные загрузки (processed, downloaded, failed)
        
    Returns:
        str: Base64 encoded изображение графика
    """
    fig = create_download_progress_chart_figure(data)
    if fig:
        return figure_to_base64(fig, dpi=150)
    return create_placeholder_chart_base64("No download data available")


def create_download_stats_chart(data: List[Dict[str, Any]]) -> str:
    """
    Создает график статистики загрузки (base64 для обратной совместимости).
    
    Args:
        data: Данные статистики загрузки
        
    Returns:
        str: Base64 encoded изображение графика
    """
    fig = create_download_stats_chart_figure(data)
    if fig:
        return figure_to_base64(fig, dpi=300)
    return create_placeholder_chart_base64("No download data available")

