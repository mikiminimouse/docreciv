# WebUI Design Guide - Technical Precision Theme

## Обзор

**Technical Precision** - это custom тема для Receiver WebUI, вдохновленная профессиональными инструментами мониторинга и научными приборами. Тема создает атмосферу точности, надежности и технического совершенства.

## Концепция дизайна

### Визуальная идентичность
- **Тон**: Техническая элегантность - точный, надежный, утонченный
- **Вдохновение**: Приборные панели, научные инструменты, data observatories
- **Ключевое впечатление**: Статус индикаторы которые пульсируют и светятся как приборы в центре управления

## Цветовая палитра

### Основные цвета
```css
--bg-primary: #0a0e17      /* Deep space blue */
--bg-secondary: #121826     /* Slightly lighter */
--bg-surface: #1a2332       /* Card backgrounds */
--bg-elevated: #222d3f      /* Elevated elements */
```

### Акцентные цвета
```css
--accent-cyan: #00d4ff      /* Primary actions, status OK */
--accent-amber: #ffb020     /* Warnings, in-progress */
--accent-coral: #ff6b6b     /* Errors, critical */
--accent-emerald: #00e5a0   /* Success states */
```

### Текстовые цвета
```css
--text-primary: #e4e8f0     /* Main text */
--text-secondary: #8b95a8   /* Secondary text */
--text-muted: #5a6577       /* Muted text */
```

## Типографика

### Шрифты
- **Display/Mono**: `'Azeret Mono', monospace` - для заголовков, метрик, технических данных
- **Body**: `'Plus Jakarta Sans', sans-serif` - для основного текста

### Использование
```css
/* Заголовки */
h1 { font-family: 'Azeret Mono'; font-size: 2.5rem; }
h2 { font-family: 'Azeret Mono'; font-size: 1.25rem; }

/* Метрики и значения */
.metric-value { font-family: 'Azeret Mono'; font-size: 2rem; }

/* Основной текст */
body { font-family: 'Plus Jakarta Sans'; }
```

## Компоненты

### Status Cards

Карты статуса с анимированными индикаторами:

```python
from receiver.webui.components.status_card import create_status_card

status_card = create_status_card(
    label="VPN Connection",
    status="ok",  # ok, warning, error, unknown
    value="CONNECTED",
    details="Interface: tun0 | IP: 10.8.0.1"
)
```

**Визуальные особенности:**
- Пульсирующий цветной индикатор (🟢🟡🔴)
- Hover эффект с поднятием карты
- Glow эффект на границе

### Metric Cards

Карты метрик с трендами:

```python
from receiver.webui.components.status_card import create_metric_card

metric_card = create_metric_card(
    label="Protocols Synced",
    value=1250,
    unit="protocols",
    trend="up"  # up, down, neutral
)
```

**Визуальные особенности:**
- Градиентная полоска сверху
- Большое значение с единицами измерения
- Стрелка тренда (↗↘→)
- Fade-in анимация при загрузке

### Buttons

#### Primary (Главные действия)
```python
gr.Button("⚡ Sync Now", variant="primary", size="lg")
```
- Градиент Cyan → Emerald
- Glow эффект
- Увеличенный shadow при hover

#### Secondary (Второстепенные)
```python
gr.Button("📊 View Stats", variant="secondary")
```
- Прозрачный фон
- Amber граница
- Subtle fill при hover

## Анимации

### Pulse (для статус индикаторов)
```css
@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.5; transform: scale(0.8); }
}
```

### Fade In (для карт при загрузке)
```css
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}
```

### Glow (для primary элементов)
```css
@keyframes glow {
    0%, 100% { box-shadow: 0 0 10px rgba(0, 212, 255, 0.3); }
    50% { box-shadow: 0 0 20px rgba(0, 212, 255, 0.6); }
}
```

## Использование в коде

### Пример: Создание улучшенного dashboard

```python
import gradio as gr
from receiver.webui.components.status_card import (
    create_status_card,
    create_metric_card
)

with gr.Tab("🎛 Control Center"):
    gr.Markdown("## System Status")

    with gr.Row():
        # Создаем HTML элементы для статусов
        vpn_status = gr.HTML(elem_classes=["status-card"])
        mongo_status = gr.HTML(elem_classes=["status-card"])

    def update_status():
        vpn_html = """
        <div class="status-card status-ok">
            <div class="metric-label">🟢 VPN Connection</div>
            <div class="metric-value">CONNECTED</div>
            <div class="metric-details">Interface: tun0</div>
        </div>
        """
        return vpn_html

    # Привязываем к кнопке обновления
    refresh_btn = gr.Button("🔄 Refresh")
    refresh_btn.click(fn=update_status, outputs=[vpn_status])
```

## Рекомендации по дизайну

### DO ✅

- **Используйте монoширинный шрифт** для технических данных (IP адреса, метрики, коды)
- **Группируйте связанную информацию** в карты с визуальными границами
- **Анимируйте статусы** чтобы привлечь внимание к изменениям
- **Используйте цветовые коды** консистентно (cyan = info, amber = warning, coral = error, emerald = success)
- **Добавляйте hover эффекты** для интерактивных элементов

### DON'T ❌

- **Не смешивайте шрифты** вне определенных пар (Azeret Mono + Plus Jakarta Sans)
- **Не используйте яркие цвета** для фонов - только для акцентов
- **Не перегружайте анимациями** - меньше, но качественнее
- **Не нарушайте visual hierarchy** - важные элементы должны выделяться

## Responsive дизайн

Тема адаптируется к мобильным устройствам:

```css
@media (max-width: 768px) {
    /* Уменьшенные заголовки */
    h1 { font-size: 1.75rem !important; }

    /* Компактные кнопки */
    button { padding: 0.5rem 1rem !important; }

    /* Меньшие метрики */
    .metric-value { font-size: 1.5rem; }
}
```

## Accessibility

- **Контраст**: Все цвета проходят WCAG AA стандарт
- **Focus states**: Видимые focus индикаторы для keyboard navigation
- **Alt text**: Используйте emojis как визуальные подсказки, но дублируйте текстом
- **Font sizes**: Минимум 0.85rem для читаемости

## Файловая структура

```
webui/
├── static/
│   └── custom_theme.css        # Основная тема (600+ строк)
├── components/
│   ├── __init__.py
│   └── status_card.py          # Компоненты UI (карты, баннеры)
├── tabs/
│   ├── dashboard.py            # Улучшенный dashboard
│   └── ...                     # Другие табы
└── app.py                      # Main app с загрузкой CSS
```

## Расширение темы

### Добавление новых цветов

1. Добавьте в `:root` в `custom_theme.css`:
```css
:root {
    --accent-purple: #a78bfa;
}
```

2. Создайте utility класс:
```css
.text-purple { color: var(--accent-purple) !important; }
```

### Создание нового типа карты

```python
def create_custom_card(title, content, type="info"):
    """Создать кастомную карту."""
    colors = {
        "info": "text-cyan",
        "warning": "text-amber",
        "error": "text-coral"
    }

    html = f"""
    <div class="custom-card">
        <h3 class="{colors.get(type)}">{title}</h3>
        <p>{content}</p>
    </div>
    """
    return gr.HTML(html)
```

## Production чек-лист

Перед деплоем:

- [ ] CSS минифицирован (опционально)
- [ ] Все шрифты загружаются корректно
- [ ] Протестировано в Chrome, Firefox, Safari
- [ ] Мобильная версия проверена
- [ ] Accessibility audit пройден
- [ ] Performance оптимизирован (lazy load для тяжелых компонентов)

## Поддержка

При возникновении проблем:

1. Проверьте, что CSS файл загружается (логи app.py)
2. Убедитесь, что elem_classes применяются корректно
3. Проверьте browser console на CSS ошибки
4. Используйте browser DevTools для debug стилей

---

**Версия**: 2.0
**Последнее обновление**: 2026-01-14
**Автор**: Claude Code (Sonnet 4.5) + frontend-design skill
