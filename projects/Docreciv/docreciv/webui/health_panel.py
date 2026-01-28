"""
Модуль для панели Health Check в WebUI.
Содержит функции для проверки здоровья компонентов и форматирования результатов.
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import gradio as gr

from docreciv.sync_db.health_checks import (
    check_vpn_connectivity,
    check_remote_mongodb_connectivity,
    check_local_mongodb_connectivity,
    check_environment_variables,
    check_ssl_certificate_validity,
    run_comprehensive_health_check,
    HealthCheckResult
)
from docreciv.core.config import get_config
from docreciv.vpn_utils import check_zakupki_access, get_vpn_status, is_openvpn_running, is_vpn_interface_up

logger = logging.getLogger(__name__)


def get_status_color(status: str) -> str:
    """Возвращает цвет для статуса."""
    if status == "healthy":
        return "#28a745"  # Зеленый
    elif status == "degraded":
        return "#ffc107"  # Желтый
    else:
        return "#dc3545"  # Красный


def get_status_icon(status: str) -> str:
    """Возвращает иконку для статуса."""
    if status == "healthy":
        return "🟢"
    elif status == "degraded":
        return "🟡"
    else:
        return "🔴"


def format_health_log(result: HealthCheckResult, timestamp: datetime) -> str:
    """Форматирует результат health check для вывода в лог."""
    lines = []
    lines.append(f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {get_status_icon(result.status)} {result.name}")
    lines.append(f"  Status: {result.status.upper()}")
    lines.append(f"  Message: {result.message}")
    
    if result.details:
        lines.append("  Details:")
        for key, value in result.details.items():
            if isinstance(value, (int, float)):
                lines.append(f"    {key}: {value}")
            elif isinstance(value, str) and len(value) < 100:
                lines.append(f"    {key}: {value}")
            else:
                lines.append(f"    {key}: {str(value)[:100]}...")
    
    lines.append("")
    return "\n".join(lines)


def check_vpn_health() -> Tuple[str, str, str]:
    """Проверяет VPN здоровье и возвращает статус, сообщение и цвет."""
    try:
        result = check_vpn_connectivity()
        color = get_status_color(result.status)
        icon = get_status_icon(result.status)
        
        # Добавляем детальную информацию о VPN
        vpn_status = get_vpn_status()
        details_parts = []
        
        if "openvpn_running" in result.details:
            details_parts.append(f"OpenVPN: {'✓' if result.details.get('openvpn_running') else '✗'}")
        if "interface_up" in result.details:
            details_parts.append(f"Interface: {'✓' if result.details.get('interface_up') else '✗'}")
        
        status_text = f"{icon} VPN Health: {result.status.upper()}"
        message = result.message
        
        if details_parts:
            message += f" | {' | '.join(details_parts)}"
        
        if result.details:
            # Добавляем информацию о маршрутах если есть
            if "routes" in result.details:
                routes = result.details["routes"]
                if isinstance(routes, dict):
                    route_status = ", ".join([f"{k}: {'✓' if v else '✗'}" for k, v in list(routes.items())[:3]])
                    if route_status:
                        message += f" | Routes: {route_status}"
        
        return status_text, message, color
    except Exception as e:
        logger.error(f"Error checking VPN health: {e}")
        return "🔴 VPN Health: ERROR", f"Error: {e}", "#dc3545"


def check_remote_mongo_health() -> Tuple[str, str, str]:
    """Проверяет Remote MongoDB здоровье и возвращает статус, сообщение и цвет."""
    try:
        result = check_remote_mongodb_connectivity()
        color = get_status_color(result.status)
        icon = get_status_icon(result.status)
        status_text = f"{icon} Remote MongoDB: {result.status.upper()}"
        message = result.message
        if result.details:
            details_str = ", ".join([f"{k}: {v}" for k, v in result.details.items() if isinstance(v, (int, float, str)) and len(str(v)) < 50])
            if details_str:
                message += f" ({details_str})"
        return status_text, message, color
    except Exception as e:
        logger.error(f"Error checking remote MongoDB health: {e}")
        return "🔴 Remote MongoDB: ERROR", f"Error: {e}", "#dc3545"


def check_local_mongo_health() -> Tuple[str, str, str]:
    """Проверяет Local MongoDB здоровье и возвращает статус, сообщение и цвет."""
    try:
        result = check_local_mongodb_connectivity()
        color = get_status_color(result.status)
        icon = get_status_icon(result.status)
        status_text = f"{icon} Local MongoDB: {result.status.upper()}"
        message = result.message
        if result.details:
            details_str = ", ".join([f"{k}: {v}" for k, v in result.details.items() if isinstance(v, (int, float, str)) and len(str(v)) < 50])
            if details_str:
                message += f" ({details_str})"
        return status_text, message, color
    except Exception as e:
        logger.error(f"Error checking local MongoDB health: {e}")
        return "🔴 Local MongoDB: ERROR", f"Error: {e}", "#dc3545"


def check_all_health_components() -> Tuple[str, str, str, str, str, str]:
    """Проверяет все компоненты здоровья и возвращает результаты."""
    vpn_status, vpn_msg, vpn_color = check_vpn_health()
    remote_status, remote_msg, remote_color = check_remote_mongo_health()
    local_status, local_msg, local_color = check_local_mongo_health()
    
    return vpn_status, vpn_msg, vpn_color, remote_status, remote_msg, remote_color, local_status, local_msg, local_color


def check_environment_health() -> Tuple[str, str, str]:
    """Проверяет Environment Variables здоровье и возвращает статус, сообщение и цвет."""
    try:
        result = check_environment_variables()
        color = get_status_color(result.status)
        icon = get_status_icon(result.status)
        status_text = f"{icon} Environment Variables: {result.status.upper()}"
        message = result.message
        if result.details:
            # Показываем количество проверенных переменных
            if "checked_variables" in result.details:
                message += f" ({result.details['checked_variables']} переменных проверено)"
            elif "missing_variables" in result.details:
                missing = result.details["missing_variables"]
                if isinstance(missing, list) and len(missing) > 0:
                    message += f" (отсутствуют: {', '.join(missing[:3])}{'...' if len(missing) > 3 else ''})"
        return status_text, message, color
    except Exception as e:
        logger.error(f"Error checking environment health: {e}")
        return "🔴 Environment Variables: ERROR", f"Error: {e}", "#dc3545"


def check_ssl_health() -> Tuple[str, str, str]:
    """Проверяет SSL Certificate здоровье и возвращает статус, сообщение и цвет."""
    try:
        config = get_config()
        if not config.sync_db.remote_mongo.ssl_cert:
            return "🔴 SSL Certificate: NOT CONFIGURED", "Путь к SSL сертификату не настроен", "#dc3545"
        
        result = check_ssl_certificate_validity(config.sync_db.remote_mongo.ssl_cert)
        color = get_status_color(result.status)
        icon = get_status_icon(result.status)
        status_text = f"{icon} SSL Certificate: {result.status.upper()}"
        message = result.message
        if result.details:
            # Добавляем информацию о сертификате если есть
            details_str = ", ".join([f"{k}: {v}" for k, v in result.details.items() if isinstance(v, (int, float, str)) and len(str(v)) < 50])
            if details_str:
                message += f" ({details_str})"
        return status_text, message, color
    except Exception as e:
        logger.error(f"Error checking SSL health: {e}")
        return "🔴 SSL Certificate: ERROR", f"Error: {e}", "#dc3545"


def check_zakupki_health() -> Tuple[str, str, str]:
    """Проверяет доступность zakupki.gov.ru и возвращает статус, сообщение и цвет."""
    try:
        accessible, url, response_time = check_zakupki_access()
        if accessible:
            status_text = "🟢 Zakupki.gov.ru: ACCESSIBLE"
            message = f"Доступен (URL: {url})"
            if response_time:
                message += f", время отклика: {response_time:.2f}s"
            color = "#28a745"
        else:
            status_text = "🔴 Zakupki.gov.ru: UNAVAILABLE"
            message = f"Недоступен"
            if url:
                message += f" (URL: {url})"
            if response_time:
                message += f", время отклика: {response_time:.2f}s"
            color = "#dc3545"
        return status_text, message, color
    except Exception as e:
        logger.error(f"Error checking zakupki health: {e}")
        return "🔴 Zakupki.gov.ru: ERROR", f"Error: {e}", "#dc3545"


def run_individual_check(check_type: str) -> str:
    """Запускает индивидуальную проверку компонента."""
    timestamp = datetime.now()
    
    try:
        if check_type == "vpn":
            result = check_vpn_connectivity()
        elif check_type == "zakupki":
            # Специальная проверка для zakupki.gov.ru
            accessible, url, response_time = check_zakupki_access()
            if accessible:
                result = HealthCheckResult(
                    name="Zakupki.gov.ru Access",
                    status="healthy",
                    message=f"Successfully accessed zakupki.gov.ru",
                    details={
                        "url": url or "https://zakupki.gov.ru",
                        "response_time_seconds": round(response_time, 2) if response_time else None
                    }
                )
            else:
                result = HealthCheckResult(
                    name="Zakupki.gov.ru Access",
                    status="unhealthy",
                    message=f"zakupki.gov.ru is not accessible (VPN may be required)",
                    details={
                        "url": url or "https://zakupki.gov.ru",
                        "response_time_seconds": round(response_time, 2) if response_time else None
                    }
                )
        elif check_type == "remote_mongodb":
            result = check_remote_mongodb_connectivity()
        elif check_type == "local_mongodb":
            result = check_local_mongodb_connectivity()
        elif check_type == "environment":
            result = check_environment_variables()
        elif check_type == "ssl_certificate":
            config = get_config()
            if config.sync_db.remote_mongo.ssl_cert:
                result = check_ssl_certificate_validity(config.sync_db.remote_mongo.ssl_cert)
            else:
                return format_health_log(
                    HealthCheckResult(
                        name="SSL Certificate",
                        status="unhealthy",
                        message="SSL certificate path not configured"
                    ),
                    timestamp
                )
        elif check_type == "all":
            results = run_comprehensive_health_check()
            log_lines = []
            for check_name, result in results.items():
                log_lines.append(format_health_log(result, timestamp))
            return "\n".join(log_lines)
        else:
            return f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] ❌ Unknown check type: {check_type}\n"
        
        return format_health_log(result, timestamp)
    except Exception as e:
        logger.error(f"Error running check {check_type}: {e}")
        import traceback
        error_details = traceback.format_exc()
        return f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] ❌ Error: {e}\n\n{error_details}\n"


def get_comprehensive_health_log() -> str:
    """Получает полный лог всех health checks."""
    timestamp = datetime.now()
    results = run_comprehensive_health_check()
    
    log_lines = []
    log_lines.append(f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] ===== COMPREHENSIVE HEALTH CHECK =====\n")
    
    for check_name, result in results.items():
        log_lines.append(format_health_log(result, timestamp))
    
    # Подсчет статистики
    healthy_count = sum(1 for r in results.values() if r.is_healthy())
    degraded_count = sum(1 for r in results.values() if r.is_degraded())
    unhealthy_count = sum(1 for r in results.values() if r.is_unhealthy())
    total_count = len(results)
    
    log_lines.append(f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] ===== SUMMARY =====\n")
    log_lines.append(f"  Total checks: {total_count}\n")
    log_lines.append(f"  🟢 Healthy: {healthy_count}\n")
    log_lines.append(f"  🟡 Degraded: {degraded_count}\n")
    log_lines.append(f"  🔴 Unhealthy: {unhealthy_count}\n")
    log_lines.append(f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] ===== END OF CHECK =====\n")
    
    return "\n".join(log_lines)

