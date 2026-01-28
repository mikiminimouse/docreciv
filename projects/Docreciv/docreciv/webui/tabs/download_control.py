"""
Download Control Tab for WebUI
"""

import gradio as gr
from datetime import datetime, timedelta
from docreciv.webui.handlers.download_handlers import download_protocols_advanced_handler


def create_download_control_tab():
    """Create the download control tab."""
    with gr.Tab("💾 Download Control"):
        gr.Markdown("## Управление загрузкой протоколов")
        gr.Markdown("Ручной запуск загрузки с просмотром метрик, аналитики и статистики")
        
        with gr.Row():
            with gr.Column():
                gr.Markdown("### 📅 Фильтр по дате")
                download_filter_type = gr.Radio(
                    choices=["Все ожидающие", "Последний день", "Последняя неделя", "Последний месяц", "С последней загрузки", "Произвольный период"],
                    value="Все ожидающие",
                    label="Тип фильтра",
                    info="Выберите период для загрузки протоколов"
                )
                download_from_date = gr.DateTime(
                    label="От (дата и время)",
                    value=None,
                    info="Начальная дата и время для произвольного периода",
                    visible=False
                )
                download_to_date = gr.DateTime(
                    label="До (дата и время)",
                    value=None,
                    info="Конечная дата и время для произвольного периода",
                    visible=False
                )
            
            with gr.Column():
                gr.Markdown("### ⚙️ Параметры загрузки")
                download_max_units = gr.Number(
                    label="🔢 Макс. UNIT за запуск (0 = без лимита)",
                    value=0,
                    precision=0,
                    minimum=0,
                    info="Максимальное количество UNIT для обработки за один запуск"
                )
                download_max_urls_per_unit = gr.Number(
                    label="🔗 Макс. URL на UNIT",
                    value=15,
                    precision=0,
                    minimum=1,
                    info="Максимальное количество URL для загрузки из одного протокола"
                )
                download_force_reload = gr.Checkbox(
                    label="🔄 Принудительная перезагрузка",
                    value=False,
                    info="Перезагрузить UNIT, даже если они уже существуют"
                )
                download_skip_existing = gr.Checkbox(
                    label="⏩ Пропускать существующие UNIT",
                    value=True,
                    info="Пропускать UNIT, если их директории уже существуют (и не включена принудительная перезагрузка)"
                )
                download_dry_run = gr.Checkbox(
                    label="🧪 Dry-run (тестовый режим)",
                    value=False,
                    info="Выполнить без реальной загрузки файлов и изменений в БД"
                )
        
        download_button = gr.Button("📥 Запустить загрузку", variant="primary", size="lg")
        
        with gr.Row():
            download_status = gr.Textbox(label="📊 Статус загрузки", interactive=False, lines=2)
            download_metrics = gr.Textbox(label="📈 Детальные метрики", interactive=False, lines=15)
        
        with gr.Row():
            download_errors = gr.Textbox(label="⚠️ Ошибки и предупреждения", interactive=False, lines=10)
            download_chart = gr.Image(label="📊 График результатов", type="pil", height=400)
        
        view_download_stats_btn = gr.Button("📊 Просмотр статистики загрузки", variant="secondary")
        download_stats_display = gr.Textbox(label="📈 Историческая статистика", interactive=False, lines=10)
        
        # Функция для обновления видимости полей дат
        def update_date_fields_visibility(filter_type):
            """Обновить видимость полей дат в зависимости от типа фильтра."""
            if filter_type == "Произвольный период":
                return gr.update(visible=True), gr.update(visible=True)
            else:
                return gr.update(visible=False), gr.update(visible=False)
        
        download_filter_type.change(
            fn=update_date_fields_visibility,
            inputs=[download_filter_type],
            outputs=[download_from_date, download_to_date]
        )
        
        download_button.click(
            fn=download_protocols_advanced_handler,
            inputs=[
                download_filter_type,
                download_from_date,
                download_to_date,
                download_max_units,
                download_max_urls_per_unit,
                download_force_reload,
                download_skip_existing,
                download_dry_run
            ],
            outputs=[download_status, download_metrics, download_errors, download_chart]
        )





