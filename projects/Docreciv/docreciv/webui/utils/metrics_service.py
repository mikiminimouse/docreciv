"""
Единый интерфейс для работы с метриками.
Объединяет SyncAnalytics, metrics_aggregator и metrics_visualization.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from docreciv.sync_db.analytics import SyncAnalytics
from docreciv.webui.utils.metrics_aggregator import (
    aggregate_by_days_of_week,
    aggregate_by_days_of_month,
    aggregate_by_days_last_3_months,
    aggregate_by_weeks_last_3_months,
    aggregate_by_weeks_per_year,
    aggregate_by_months_per_year,
    aggregate_by_days_simple
)
from docreciv.webui.utils.metrics_visualization import (
    get_sync_metrics_data,
    create_metrics_chart,
    create_metrics_table,
    get_metrics_visualization,
    figure_to_base64,
    figure_to_pil_image,
    create_sync_trend_chart_figure,
    create_performance_chart_figure,
    create_error_distribution_chart_figure,
    create_sync_progress_chart_figure,
    create_download_progress_chart_figure,
    create_download_stats_chart_figure
)

logger = logging.getLogger(__name__)


class MetricsService:
    """
    Единый сервис для работы с метриками синхронизации и загрузки.
    Предоставляет унифицированный интерфейс для получения, агрегации и визуализации метрик.
    """
    
    def __init__(self):
        """Инициализация сервиса метрик."""
        self._analytics: Optional[SyncAnalytics] = None
    
    def _get_analytics(self) -> SyncAnalytics:
        """Получить или создать экземпляр SyncAnalytics."""
        if self._analytics is None:
            self._analytics = SyncAnalytics()
        return self._analytics
    
    def get_metrics(
        self,
        days: int = 365,
        collection: str = "protocols"
    ) -> List[Dict[str, Any]]:
        """
        Получить метрики синхронизации.
        
        Args:
            days: Количество дней истории
            collection: Название коллекции (пока только "protocols")
            
        Returns:
            Список записей с метриками
        """
        try:
            return get_sync_metrics_data(days)
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            return []
    
    def aggregate_metrics(
        self,
        data: List[Dict[str, Any]],
        period_type: str,
        days: Optional[int] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Агрегировать метрики по указанному периоду.
        
        Args:
            data: Список записей с метриками
            period_type: Тип периода агрегации:
                - "days_of_week" - по дням недели
                - "days_of_month" - по дням месяца
                - "days_last_3_months" - по дням за 3 месяца
                - "weeks_last_3_months" - по неделям за 3 месяца
                - "weeks_per_year" - по неделям за год
                - "months_per_year" - по месяцам за год
                - "days_simple" - простая агрегация по дням
            days: Количество дней для фильтрации (для days_simple)
            
        Returns:
            Словарь с агрегированными данными
        """
        try:
            if period_type == "days_of_week":
                return aggregate_by_days_of_week(data)
            elif period_type == "days_of_month":
                return aggregate_by_days_of_month(data)
            elif period_type == "days_last_3_months":
                return aggregate_by_days_last_3_months(data)
            elif period_type == "weeks_last_3_months":
                return aggregate_by_weeks_last_3_months(data)
            elif period_type == "weeks_per_year":
                return aggregate_by_weeks_per_year(data)
            elif period_type == "months_per_year":
                return aggregate_by_months_per_year(data)
            elif period_type == "days_simple":
                return aggregate_by_days_simple(data, days)
            else:
                logger.warning(f"Unknown period type: {period_type}, using days_last_3_months")
                return aggregate_by_days_last_3_months(data)
        except Exception as e:
            logger.error(f"Error aggregating metrics: {e}")
            return {}
    
    def visualize_metrics(
        self,
        filter_type: str,
        days: int = 365,
        output_format: str = "figure"
    ) -> Tuple[Optional[Any], List[List[str]], List[str]]:
        """
        Визуализировать метрики (график + таблица).
        
        Args:
            filter_type: Тип фильтра агрегации (как в UI)
            days: Количество дней истории
            output_format: Формат вывода:
                - "figure" - matplotlib Figure
                - "base64" - base64 строка
                - "pil" - PIL Image
                
        Returns:
            Кортеж (график, данные таблицы, заголовки таблицы)
        """
        try:
            # Маппинг типов фильтров UI на типы агрегации
            filter_to_period = {
                "По дням недели": "days_of_week",
                "По дням месяца": "days_of_month",
                "По дням за 3 месяца": "days_last_3_months",
                "По неделям за 3 месяца": "weeks_last_3_months",
                "По неделям за год": "weeks_per_year",
                "По месяцам за год": "months_per_year"
            }
            
            period_type = filter_to_period.get(filter_type, "days_last_3_months")
            
            # Получаем данные
            raw_data = self.get_metrics(days)
            if not raw_data:
                return None, [], []
            
            # Агрегируем
            aggregated = self.aggregate_metrics(raw_data, period_type)
            
            # Создаем график
            chart = create_metrics_chart(aggregated, filter_type, "inserted")
            
            # Создаем таблицу
            table_data, table_headers = create_metrics_table(aggregated, filter_type)
            
            # Конвертируем в нужный формат
            if output_format == "base64" and chart:
                chart = figure_to_base64(chart, dpi=100)
            elif output_format == "pil" and chart:
                chart = figure_to_pil_image(chart)
            
            return chart, table_data, table_headers
            
        except Exception as e:
            logger.error(f"Error visualizing metrics: {e}")
            return None, [], []
    
    def get_sync_trends(self, days: int = 30) -> Dict[str, Any]:
        """
        Получить тренды синхронизации (совместимость с SyncAnalytics).
        
        Args:
            days: Количество дней для анализа
            
        Returns:
            Словарь с трендами
        """
        try:
            analytics = self._get_analytics()
            return analytics.get_sync_trends(days)
        except Exception as e:
            logger.error(f"Error getting sync trends: {e}")
            return {}
    
    def create_sync_progress_chart(
        self,
        data: Dict[str, Any],
        date_str: str = "",
        output_format: str = "base64"
    ) -> Any:
        """
        Создать график прогресса синхронизации.
        
        Args:
            data: Данные синхронизации (scanned, inserted, skipped, errors)
            date_str: Дата синхронизации
            output_format: Формат вывода ("figure", "base64", "pil")
            
        Returns:
            График в указанном формате
        """
        try:
            fig = create_sync_progress_chart_figure(data, date_str)
            if not fig:
                return None
            
            if output_format == "base64":
                return figure_to_base64(fig, dpi=150)
            elif output_format == "pil":
                return figure_to_pil_image(fig)
            else:
                return fig
        except Exception as e:
            logger.error(f"Error creating sync progress chart: {e}")
            return None
    
    def create_download_progress_chart(
        self,
        data: Dict[str, Any],
        output_format: str = "base64"
    ) -> Any:
        """
        Создать график прогресса загрузки.
        
        Args:
            data: Данные загрузки (processed, downloaded, failed)
            output_format: Формат вывода ("figure", "base64", "pil")
            
        Returns:
            График в указанном формате
        """
        try:
            fig = create_download_progress_chart_figure(data)
            if not fig:
                return None
            
            if output_format == "base64":
                return figure_to_base64(fig, dpi=150)
            elif output_format == "pil":
                return figure_to_pil_image(fig)
            else:
                return fig
        except Exception as e:
            logger.error(f"Error creating download progress chart: {e}")
            return None
    
    def close(self) -> None:
        """Закрыть соединения и освободить ресурсы."""
        if self._analytics:
            self._analytics.close()
            self._analytics = None

