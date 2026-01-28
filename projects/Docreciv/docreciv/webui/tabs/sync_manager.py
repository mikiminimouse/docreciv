"""
Sync Manager Tab - управляемая синхронизация протоколов.
"""

import gradio as gr
import io
import logging
from datetime import datetime, timedelta

from docreciv.webui.handlers.sync_manager_handlers import (
    sync_manager_start_sync,
    sync_manager_get_status,
    sync_manager_cancel,
    sync_manager_get_cursor_state,
    sync_manager_get_cursor_date,
    sync_manager_get_recent_runs
)

logger = logging.getLogger(__name__)


def create_sync_manager_tab():
    """
    Создать таб Sync Manager.
    
    Функция создает таб внутри текущего контекста gr.Blocks.
    """
    with gr.Tab("🔁 Sync Manager"):
        gr.Markdown("## Менеджер синхронизации БД")
        gr.Markdown("Управляемая синхронизация протоколов с поддержкой различных режимов и мониторингом прогресса")
        
        with gr.Row():
            with gr.Column():
                gr.Markdown("### 📂 Коллекция")
                sync_collection = gr.Dropdown(
                    choices=["protocols"],
                    value="protocols",
                    label="Коллекция",
                    interactive=False,
                    info="Текущая коллекция для синхронизации"
                )
                cursor_state_btn = gr.Button("📊 Получить состояние курсора", variant="secondary")
                cursor_state_display = gr.Textbox(label="Состояние курсора", interactive=False, lines=5)
            
            with gr.Column():
                gr.Markdown("### ⏱️ Режим синхронизации")
                sync_mode = gr.Radio(
                    choices=["incremental", "range", "backfill", "replay"],
                    value="incremental",
                    label="Режим",
                    info="incremental: от последнего курсора до текущей даты\nrange: произвольный диапазон дат\nbackfill: догрузка исторических данных\nreplay: переигрывание периода"
                )
        
        with gr.Row():
            with gr.Column():
                gr.Markdown("### 📅 Диапазон дат")
                sync_date_info = gr.Markdown("", visible=True)
                sync_from_date = gr.DateTime(
                    label="От (дата и время)",
                    value=None,
                    visible=False,
                    info="Начальная дата"
                )
                sync_to_date = gr.DateTime(
                    label="До (дата и время)",
                    value=None,
                    visible=False,
                    info="Конечная дата"
                )
                sync_cursor_info = gr.Markdown("", visible=False)
            
            with gr.Column():
                gr.Markdown("### ⚙️ Параметры")
                sync_limit_batch = gr.Checkbox(
                    label="Ограничить размер пакета",
                    value=False,
                    info="Включить ограничение количества документов в пакете"
                )
                sync_batch_size = gr.Number(
                    label="Размер пакета",
                    value=1000,
                    precision=0,
                    minimum=1,
                    maximum=10000,
                    visible=False,
                    info="Количество документов в пакете"
                )
                sync_dry_run = gr.Checkbox(
                    label="Dry-run (тестовый режим)",
                    value=False,
                    info="Выполнить без реальных изменений"
                )
                sync_write_mode = gr.Radio(
                    choices=["merge", "overwrite"],
                    value="merge",
                    label="Режим записи",
                    info="merge: объединение данных\noverwrite: перезапись данных"
                )
        
        with gr.Row():
            sync_start_btn = gr.Button("🚀 Запустить синхронизацию", variant="primary", size="lg")
            sync_cancel_btn = gr.Button("⏹️ Отменить синхронизацию", variant="stop", size="lg")
        
        with gr.Row():
            sync_run_id = gr.Textbox(
                label="Run ID",
                value="",
                placeholder="Введите Run ID для отслеживания",
                info="Идентификатор запуска синхронизации"
            )
            sync_status_btn = gr.Button("🔄 Обновить статус", variant="secondary")
        
        with gr.Row():
            sync_manager_status = gr.Textbox(label="📊 Статус синхронизации", interactive=False, lines=5)
            sync_manager_progress_value = gr.Number(label="Прогресс (%)", value=0.0, interactive=False, precision=1)
        
        with gr.Row():
            sync_manager_details = gr.Textbox(label="📈 Детальная информация", interactive=False, lines=15)
        
        with gr.Row():
            recent_runs_btn = gr.Button("📋 Последние запуски", variant="secondary")
            recent_runs_display = gr.Textbox(label="История запусков", interactive=False, lines=10)
        
        # Обработчики событий
        def update_ui_for_mode(mode, cursor_state_text="", cursor_date=None):
            """Динамически обновить UI в зависимости от выбранного режима."""
            if mode == "incremental":
                # Incremental: автоматически установить "От" = последний курсор, скрыть "До"
                from_date_value = cursor_date if cursor_date else None
                return (
                    gr.update(visible=True, value=from_date_value),  # sync_from_date - показываем и устанавливаем курсор
                    gr.update(visible=False),  # sync_to_date
                    gr.update(visible=True, value="**Режим incremental:**\nСинхронизация от последнего курсора до текущей даты.\nДата 'От' установлена автоматически из курсора."),  # sync_date_info
                    gr.update(visible=True, value=cursor_state_text)  # sync_cursor_info
                )
            elif mode == "range":
                # Range: показать оба поля дат, обязательные
                return (
                    gr.update(visible=True),  # sync_from_date
                    gr.update(visible=True),  # sync_to_date
                    gr.update(visible=True, value="**Режим range:**\nУкажите диапазон дат для синхронизации.\nОба поля обязательны."),  # sync_date_info
                    gr.update(visible=False)  # sync_cursor_info
                )
            elif mode == "backfill":
                # Backfill: показать поле "От", "До" = последний курсор (автоматически)
                to_date_value = cursor_date if cursor_date else None
                return (
                    gr.update(visible=True),  # sync_from_date
                    gr.update(visible=True, value=to_date_value),  # sync_to_date - устанавливаем из курсора
                    gr.update(visible=True, value="**Режим backfill:**\nДогрузка исторических данных.\nУкажите начальную дату, конечная установлена автоматически из курсора."),  # sync_date_info
                    gr.update(visible=True, value=cursor_state_text)  # sync_cursor_info
                )
            elif mode == "replay":
                # Replay: показать оба поля дат, обязательные
                return (
                    gr.update(visible=True),  # sync_from_date
                    gr.update(visible=True),  # sync_to_date
                    gr.update(visible=True, value="**Режим replay:**\nПереигрывание уже синхронизированного периода.\nОба поля обязательны."),  # sync_date_info
                    gr.update(visible=False)  # sync_cursor_info
                )
            else:
                return (
                    gr.update(visible=False),
                    gr.update(visible=False),
                    gr.update(visible=True, value=""),
                    gr.update(visible=False)
                )
        
        def get_cursor_state_for_ui():
            """Получить состояние курсора для отображения в UI."""
            try:
                cursor_text = sync_manager_get_cursor_state()
                if cursor_text and "Последнее значение курсора:" in cursor_text:
                    # Извлекаем дату курсора
                    for line in cursor_text.split("\n"):
                        if "Последнее значение курсора:" in line:
                            cursor_date = line.split("Последнее значение курсора:")[1].strip()
                            return f"**Текущий курсор:** {cursor_date}\n\nЭто значение будет использовано как конечная дата для backfill или начальная для incremental."
                return "**Курсор не установлен**\n\nБудет использована текущая дата."
            except Exception as e:
                return f"**Ошибка получения курсора:** {e}"
        
        def update_ui_with_cursor(mode):
            """Обновить UI с учетом текущего состояния курсора."""
            cursor_text = get_cursor_state_for_ui()
            cursor_date = sync_manager_get_cursor_date()
            return update_ui_for_mode(mode, cursor_text, cursor_date)
        
        sync_mode.change(
            fn=update_ui_with_cursor,
            inputs=[sync_mode],
            outputs=[sync_from_date, sync_to_date, sync_date_info, sync_cursor_info]
        )
        
        # Обновить видимость поля размера пакета
        sync_limit_batch.change(
            fn=lambda checked: gr.update(visible=checked),
            inputs=[sync_limit_batch],
            outputs=[sync_batch_size]
        )
        
        # Обновить UI при загрузке страницы
        def init_ui():
            """Инициализировать UI при загрузке - автоматически получить курсор и установить даты."""
            cursor_text = get_cursor_state_for_ui()
            cursor_date = sync_manager_get_cursor_date()
            return update_ui_for_mode("incremental", cursor_text, cursor_date)
        
        # Автоматически загрузить состояние курсора и инициализировать UI при загрузке таба
        # Используем событие load для всего таба через demo.load в app.py
        # Здесь просто определяем функции для инициализации
        
        # Обертка для обработчика - теперь передаем datetime напрямую
        def sync_start_wrapper(mode, from_date, to_date, limit_batch, batch_size, dry_run, write_mode):
            """Обертка для запуска синхронизации - передаем datetime объекты напрямую."""
            # Если ограничение отключено, передаем None
            batch_size_value = batch_size if limit_batch and batch_size > 0 else None
            
            # Передаем datetime объекты напрямую (обработчик теперь поддерживает их)
            return sync_manager_start_sync(
                mode, from_date, to_date, batch_size_value, dry_run, write_mode
            )
        
        sync_start_btn.click(
            fn=sync_start_wrapper,
            inputs=[sync_mode, sync_from_date, sync_to_date, sync_limit_batch, sync_batch_size, sync_dry_run, sync_write_mode],
            outputs=[sync_manager_status, sync_manager_details, sync_run_id]
        )
        
        sync_status_btn.click(
            fn=sync_manager_get_status,
            inputs=[sync_run_id],
            outputs=[sync_manager_status, sync_manager_details, sync_manager_progress_value]
        )
        
        sync_cancel_btn.click(
            fn=sync_manager_cancel,
            inputs=[sync_run_id],
            outputs=[sync_manager_status]
        )
        
        cursor_state_btn.click(
            fn=sync_manager_get_cursor_state,
            outputs=[cursor_state_display]
        )
        
        def get_recent_runs_wrapper():
            """Обертка для получения последних запусков."""
            return sync_manager_get_recent_runs(10)
        
        recent_runs_btn.click(
            fn=get_recent_runs_wrapper,
            outputs=[recent_runs_display]
        )
        
        # ========== Секция визуализации метрик ==========
        gr.Markdown("---")
        gr.Markdown("## 📊 Метрики и статистика синхронизации")
        gr.Markdown("Визуализация метрик синхронизации по различным периодам для анализа производительности и выявления паттернов")
        
        with gr.Row():
            with gr.Column():
                metrics_filter_type = gr.Radio(
                    choices=[
                        "По дням недели",
                        "По дням месяца",
                        "По дням за 3 месяца",
                        "По неделям за 3 месяца",
                        "По неделям за год",
                        "По месяцам за год"
                    ],
                    value="По дням за 3 месяца",
                    label="Тип фильтра агрегации",
                    info="Выберите период для агрегации метрик"
                )
                metrics_refresh_btn = gr.Button("🔄 Обновить метрики", variant="primary")
            
            with gr.Column():
                metrics_days = gr.Number(
                    label="Количество дней истории",
                    value=365,
                    precision=0,
                    minimum=1,
                    maximum=3650,
                    info="Сколько дней истории использовать для анализа"
                )
        
        with gr.Row():
            metrics_chart = gr.Image(
                label="📈 График метрик синхронизации",
                type="pil",
                height=400
            )
        
        with gr.Row():
            metrics_table = gr.Dataframe(
                label="📋 Таблица метрик",
                headers=["Период", "Просмотрено", "Вставлено", "Пропущено", "Ошибок", "Сессий", "Среднее вставлено"],
                interactive=False,
                wrap=True
            )
        
        def update_metrics_visualization(filter_type, days):
            """Обновить визуализацию метрик."""
            try:
                from docreciv.webui.utils.metrics_visualization import (
                    get_metrics_visualization,
                    figure_to_image
                )
                from PIL import Image
                
                chart, table_data, table_headers = get_metrics_visualization(filter_type, int(days))
                
                # Конвертируем график в изображение
                image_bytes = figure_to_image(chart)
                if image_bytes:
                    image = Image.open(io.BytesIO(image_bytes))
                else:
                    image = None
                
                # Возвращаем изображение и таблицу
                return image, table_data
            except Exception as e:
                import traceback
                logger.error(f"Error updating metrics visualization: {e}\n{traceback.format_exc()}")
                return None, []
        
        metrics_refresh_btn.click(
            fn=update_metrics_visualization,
            inputs=[metrics_filter_type, metrics_days],
            outputs=[metrics_chart, metrics_table]
        )
        
        # Автоматическая загрузка метрик будет выполнена через кнопку при первом открытии
        # или можно добавить в demo.load() в app.py

