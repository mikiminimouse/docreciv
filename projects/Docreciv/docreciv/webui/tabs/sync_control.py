"""
Sync Control Tab for WebUI
"""

import gradio as gr
from docreciv.webui.services.ui_service import get_ui_service
from docreciv.webui.handlers.sync_handlers import sync_protocols_handler


def create_sync_control_tab():
    """Create the sync control tab."""
    with gr.Tab("🔄 Sync Control"):
        gr.Markdown("## Просмотр статистики синхронизации")
        gr.Markdown("""
        **Примечание:** Функционал запуска синхронизации перенесен на вкладку **🔁 Sync Manager**.
        
        На этой вкладке доступен только просмотр статистики и метрик синхронизации.
        """)
        
        with gr.Row():
            sync_status = gr.Textbox(label="📊 Статус синхронизации", interactive=False, lines=2, value="Используйте вкладку 🔁 Sync Manager для запуска синхронизации")
            sync_metrics = gr.Textbox(label="📈 Детальные метрики", interactive=False, lines=15)
        
        with gr.Row():
            sync_errors = gr.Textbox(label="⚠️ Ошибки и предупреждения", interactive=False, lines=10)
            sync_chart = gr.Image(label="📊 График результатов", type="pil", height=400)
        
        # Кнопка для просмотра статистики
        view_stats_btn = gr.Button("📊 Просмотр статистики синхронизации", variant="secondary")
        sync_stats_display = gr.Textbox(label="📈 Историческая статистика", interactive=False, lines=10)
        
        # Обработчики
        view_stats_btn.click(
            fn=sync_protocols_handler,
            inputs=[],
            outputs=[sync_status, sync_metrics, sync_errors, sync_chart]
        )





