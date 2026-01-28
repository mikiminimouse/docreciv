"""
Компоненты визуализации данных для WebUI.
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import io
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

def create_sync_trend_chart(data: Dict[str, Any]) -> str:
    """
    Создает график трендов синхронизации.
    
    Args:
        data: Данные трендов синхронизации
        
    Returns:
        str: Base64 encoded изображение графика
    """
    try:
        if not data or "daily_averages" not in data:
            return create_placeholder_chart("No sync data available")
        
        daily_data = data["daily_averages"]
        dates = list(daily_data.keys())
        
        if not dates:
            return create_placeholder_chart("No sync data available")
        
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
        
        # Конвертируем в base64
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
        img_buffer.seek(0)
        img_str = base64.b64encode(img_buffer.getvalue()).decode()
        plt.close(fig)
        
        return f"data:image/png;base64,{img_str}"
        
    except Exception as e:
        logger.error(f"Failed to create sync trend chart: {e}")
        return create_placeholder_chart(f"Error: {e}")

def create_performance_chart(data: Dict[str, Any]) -> str:
    """
    Создает график производительности.
    
    Args:
        data: Данные производительности
        
    Returns:
        str: Base64 encoded изображение графика
    """
    try:
        if not data or "daily_averages" not in data:
            return create_placeholder_chart("No performance data available")
        
        daily_data = data["daily_averages"]
        dates = list(daily_data.keys())
        
        if not dates:
            return create_placeholder_chart("No performance data available")
        
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
        
        # Конвертируем в base64
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
        img_buffer.seek(0)
        img_str = base64.b64encode(img_buffer.getvalue()).decode()
        plt.close(fig)
        
        return f"data:image/png;base64,{img_str}"
        
    except Exception as e:
        logger.error(f"Failed to create performance chart: {e}")
        return create_placeholder_chart(f"Error: {e}")

def create_error_distribution_chart(data: Dict[str, Any]) -> str:
    """
    Создает круговую диаграмму распределения ошибок.
    
    Args:
        data: Данные об ошибках
        
    Returns:
        str: Base64 encoded изображение графика
    """
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
        
        # Конвертируем в base64
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
        img_buffer.seek(0)
        img_str = base64.b64encode(img_buffer.getvalue()).decode()
        plt.close(fig)
        
        return f"data:image/png;base64,{img_str}"
        
    except Exception as e:
        logger.error(f"Failed to create error distribution chart: {e}")
        return create_placeholder_chart(f"Error: {e}")

def create_placeholder_chart(message: str) -> str:
    """
    Создает placeholder график с сообщением.
    
    Args:
        message: Сообщение для отображения
        
    Returns:
        str: Base64 encoded изображение графика
    """
    try:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, message, ha='center', va='center', transform=ax.transAxes, 
                fontsize=14, color='gray')
        ax.axis('off')
        ax.set_title('Chart Unavailable')
        
        # Конвертируем в base64
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
        img_buffer.seek(0)
        img_str = base64.b64encode(img_buffer.getvalue()).decode()
        plt.close(fig)
        
        return f"data:image/png;base64,{img_str}"
        
    except Exception as e:
        logger.error(f"Failed to create placeholder chart: {e}")
        return ""

def create_download_stats_chart(data: List[Dict[str, Any]]) -> str:
    """
    Создает график статистики загрузки.
    
    Args:
        data: Данные статистики загрузки
        
    Returns:
        str: Base64 encoded изображение графика
    """
    try:
        if not data:
            return create_placeholder_chart("No download data available")
        
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
        
        # Конвертируем в base64
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=300, bbox_inches='tight')
        img_buffer.seek(0)
        img_str = base64.b64encode(img_buffer.getvalue()).decode()
        plt.close(fig)
        
        return f"data:image/png;base64,{img_str}"
        
    except Exception as e:
        logger.error(f"Failed to create download stats chart: {e}")
        return create_placeholder_chart(f"Error: {e}")

def create_sync_progress_chart(data: Dict[str, Any], date_str: str = "") -> str:
    """
    Создает график прогресса синхронизации.
    
    Args:
        data: Данные синхронизации (scanned, inserted, skipped, errors)
        date_str: Дата синхронизации
        
    Returns:
        str: Base64 encoded изображение графика
    """
    try:
        if not data:
            return create_placeholder_chart("No sync data available")
        
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
        
        # Конвертируем в base64
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
        img_buffer.seek(0)
        img_str = base64.b64encode(img_buffer.getvalue()).decode()
        plt.close(fig)
        
        return f"data:image/png;base64,{img_str}"
        
    except Exception as e:
        logger.error(f"Failed to create sync progress chart: {e}")
        return create_placeholder_chart(f"Error: {e}")

def create_download_progress_chart(data: Dict[str, Any]) -> str:
    """
    Создает график прогресса загрузки.
    
    Args:
        data: Данные загрузки (processed, downloaded, failed)
        
    Returns:
        str: Base64 encoded изображение графика
    """
    try:
        if not data:
            return create_placeholder_chart("No download data available")
        
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
        
        # Конвертируем в base64
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight')
        img_buffer.seek(0)
        img_str = base64.b64encode(img_buffer.getvalue()).decode()
        plt.close(fig)
        
        return f"data:image/png;base64,{img_str}"
        
    except Exception as e:
        logger.error(f"Failed to create download progress chart: {e}")
        return create_placeholder_chart(f"Error: {e}")
