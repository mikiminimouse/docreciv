"""
Интерфейс управления для WebUI.
"""

import gradio as gr
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)

def create_sync_controls(sync_handler, analytics_service) -> Tuple:
    """
    Создает элементы управления для синхронизации.
    
    Args:
        sync_handler: Функция обработки синхронизации
        analytics_service: Сервис аналитики
        
    Returns:
        Tuple: Элементы управления
    """
    with gr.Group():
        gr.Markdown("## 🔄 Настройки синхронизации")
        
        with gr.Row():
            sync_date = gr.Textbox(
                label="📅 Дата синхронизации (YYYY-MM-DD)", 
                value=datetime.now().strftime("%Y-%m-%d")
            )
            sync_limit = gr.Number(
                label="🔢 Лимит записей (0 = без ограничений)", 
                value=0, 
                precision=0
            )
        
        with gr.Row():
            sync_type = gr.Radio(
                choices=[
                    ("Ежедневная синхронизация", "daily"),
                    ("Полная синхронизация", "full"),
                    ("По диапазону дат", "range")
                ],
                label="Тип синхронизации",
                value="daily"
            )
        
        with gr.Row(visible=False) as date_range_row:
            start_date = gr.Textbox(
                label="📅 Начальная дата (YYYY-MM-DD)"
            )
            end_date = gr.Textbox(
                label="📅 Конечная дата (YYYY-MM-DD)"
            )
        
        def update_date_range_visibility(sync_type):
            return gr.update(visible=sync_type == "range")
        
        sync_type.change(
            fn=update_date_range_visibility,
            inputs=sync_type,
            outputs=date_range_row
        )
        
        sync_button = gr.Button("🚀 Запустить синхронизацию")
        sync_result = gr.Textbox(label="📊 Результат синхронизации", interactive=False)
        
        # Обработчик синхронизации
        sync_button.click(
            fn=sync_handler,
            inputs=[sync_date, sync_limit],
            outputs=sync_result
        )
        
        # Кнопка для получения последних данных аналитики
        with gr.Row():
            refresh_analytics = gr.Button("🔄 Обновить аналитику")
            analytics_output = gr.Textbox(label="📈 Аналитика", lines=10, interactive=False)
        
        def get_latest_analytics():
            try:
                if analytics_service is None:
                    return "❌ Сервис аналитики недоступен"
                
                # Получаем последние данные
                recent_data = analytics_service.get_historical_sync_data(days=7)
                
                if not recent_data:
                    return "ℹ️ Нет данных за последние 7 дней"
                
                # Форматируем данные
                lines = ["📊 Аналитика за последние 7 дней:"]
                for record in recent_data:
                    lines.append(f"📅 {record.date}:")
                    lines.append(f"   🔍 Просканировано: {record.scanned_documents}")
                    lines.append(f"   💾 Вставлено: {record.inserted_documents}")
                    lines.append(f"   ⏭️  Пропущено: {record.skipped_duplicates}")
                    lines.append(f"   ⚠️  Ошибок: {record.processing_errors}")
                    lines.append(f"   ⏱️  Длительность: {record.duration_seconds:.2f} сек")
                    lines.append("")
                
                return "\n".join(lines)
            except Exception as e:
                return f"❌ Ошибка получения аналитики: {e}"
        
        refresh_analytics.click(
            fn=get_latest_analytics,
            outputs=analytics_output
        )
    
    return sync_date, sync_limit, sync_button, sync_result

def create_download_controls(download_handler) -> Tuple:
    """
    Создает элементы управления для загрузки.
    
    Args:
        download_handler: Функция обработки загрузки
        
    Returns:
        Tuple: Элементы управления
    """
    with gr.Group():
        gr.Markdown("## 💾 Настройки загрузки")
        
        with gr.Row():
            download_limit = gr.Number(
                label="🔢 Лимит загрузок (0 = без ограничений)", 
                value=0, 
                precision=0
            )
            download_date = gr.Textbox(
                label="📅 Дата протоколов (YYYY-MM-DD, опционально)"
            )
        
        download_button = gr.Button("📥 Запустить загрузку")
        download_result = gr.Textbox(label="📊 Результат загрузки", interactive=False)
        
        # Обработчик загрузки
        download_button.click(
            fn=download_handler,
            inputs=[download_limit],
            outputs=download_result
        )
    
    return download_limit, download_button, download_result

def create_configuration_controls(config) -> Tuple:
    """
    Создает элементы управления для конфигурации.
    
    Args:
        config: Объект конфигурации
        
    Returns:
        Tuple: Элементы управления
    """
    with gr.Group():
        gr.Markdown("## ⚙️ Конфигурация системы")
        
        # MongoDB настройки
        with gr.Accordion("🗄️ MongoDB Settings", open=False):
            remote_mongo_server = gr.Textbox(
                label="Remote MongoDB Server", 
                value=config.sync_db.remote_mongo.server
            )
            remote_mongo_user = gr.Textbox(
                label="Remote MongoDB User", 
                value=config.sync_db.remote_mongo.user
            )
            local_mongo_server = gr.Textbox(
                label="Local MongoDB Server", 
                value=config.sync_db.local_mongo.server
            )
            local_mongo_user = gr.Textbox(
                label="Local MongoDB User", 
                value=config.sync_db.local_mongo.user
            )
        
        # Настройки загрузчика
        with gr.Accordion("📥 Downloader Settings", open=False):
            max_urls = gr.Number(
                label="Max URLs per Protocol", 
                value=config.downloader.max_urls_per_protocol
            )
            http_timeout = gr.Number(
                label="HTTP Timeout (seconds)", 
                value=config.downloader.download_http_timeout
            )
            download_concurrency = gr.Number(
                label="Download Concurrency", 
                value=config.downloader.download_concurrency
            )
        
        # Настройки планировщика
        with gr.Accordion("⏰ Scheduler Settings", open=False):
            sync_cron = gr.Textbox(
                label="Sync Schedule (cron)", 
                value=config.scheduler.sync_schedule_cron
            )
            process_cron = gr.Textbox(
                label="Process Schedule (cron)", 
                value=config.scheduler.schedule_cron
            )
        
        with gr.Row():
            save_config_btn = gr.Button("💾 Сохранить конфигурацию")
            reset_config_btn = gr.Button("🔄 Сбросить конфигурацию")
        
        config_status = gr.Textbox(label="🔧 Статус конфигурации", interactive=False)
        
        def save_config():
            return "ℹ️ Функция сохранения конфигурации пока не реализована"
        
        def reset_config():
            return "ℹ️ Функция сброса конфигурации пока не реализована"
        
        save_config_btn.click(fn=save_config, outputs=config_status)
        reset_config_btn.click(fn=reset_config, outputs=config_status)
    
    return config_status

def create_vpn_controls(vpn_handler) -> Tuple:
    """
    Создает элементы управления для VPN.
    
    Args:
        vpn_handler: Функция обработки VPN проверки
        
    Returns:
        Tuple: Элементы управления
    """
    with gr.Group():
        gr.Markdown("## 🔒 Настройки VPN")
        
        vpn_status = gr.Textbox(label="🛡️ Статус VPN", lines=5, interactive=False)
        check_vpn_btn = gr.Button("🔍 Проверить VPN соединение")
        
        check_vpn_btn.click(fn=vpn_handler, outputs=vpn_status)
    
    return vpn_status, check_vpn_btn
