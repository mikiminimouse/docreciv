# Enhanced Sync and Downloader Components

## Overview

This document describes the enhanced synchronization and downloading components for the receiver pipeline. These components provide improved error handling, comprehensive health checks, advanced analytics, and better CLI integration.

## Enhanced SyncDB Service

### Features

1. **Improved Error Handling**: Comprehensive error handling with detailed logging and categorization
2. **Advanced Statistics**: Detailed metrics collection including URL distribution, attachment types, and processing times
3. **Enhanced Logging**: Structured logging with configurable levels
4. **Batch Processing**: Efficient batch processing of documents with configurable batch sizes
5. **Connection Management**: Robust connection handling with timeouts and retries

### Key Classes

#### EnhancedSyncService
Main service class for protocol synchronization with enhanced features:
- `sync_protocols_for_date()`: Synchronize protocols for a specific date
- `sync_protocols_for_date_range()`: Synchronize protocols for a date range
- `sync_daily_updates()`: Daily synchronization of recent protocols
- `sync_full_collection()`: Full synchronization for a specified period

#### EnhancedSyncResult
Enhanced result object with detailed metrics:
- Basic metrics (scanned, inserted, skipped, errors)
- URL distribution statistics
- Attachment type analysis
- Processing time statistics
- Error categorization

### Usage

```bash
# From command line
python -m receiver.sync_db.enhanced_service sync-date --date 2024-01-15
python -m receiver.sync_db.enhanced_service sync-range --start-date 2024-01-01 --end-date 2024-01-31
python -m receiver.sync_db.enhanced_service sync-daily
python -m receiver.sync_db.enhanced_service sync-full --days 30
```

## Health Checks Module

### Features

1. **Comprehensive Health Monitoring**: Checks for VPN, remote MongoDB, local MongoDB, and environment
2. **Detailed Reporting**: Structured health check reports with status icons
3. **Individual Checks**: Ability to run specific health checks
4. **Overall System Health**: Aggregated system health status

### Key Functions

- `check_vpn_connectivity()`: Check VPN connection to zakupki.gov.ru
- `check_remote_mongodb_connectivity()`: Check remote MongoDB connection
- `check_local_mongodb_connectivity()`: Check local MongoDB connection
- `check_environment_variables()`: Validate required environment variables
- `run_comprehensive_health_check()`: Run all health checks
- `get_overall_system_health()`: Get aggregated system health status

### Usage

```bash
# From command line
python -m receiver.sync_db.health_checks --check all
python -m receiver.sync_db.health_checks --check vpn
python -m receiver.sync_db.health_checks --check remote-mongo
```

## Analytics Module

### Features

1. **Historical Data Tracking**: Store and retrieve historical synchronization data
2. **Trend Analysis**: Analyze synchronization trends over time
3. **Comprehensive Reporting**: Generate detailed synchronization reports
4. **Data Export**: Export analytics data to JSON format

### Key Classes

#### SyncStatistics
Detailed statistics for synchronization sessions:
- Session metadata and timing
- Document processing metrics
- URL and attachment analysis
- Error categorization

#### HistoricalSyncRecord
Historical record of synchronization sessions:
- Date and basic metrics
- Duration and sync type
- Timestamp information

#### SyncAnalytics
Analytics service for synchronization data:
- `store_sync_statistics()`: Store synchronization statistics
- `get_sync_statistics()`: Retrieve synchronization statistics
- `get_historical_sync_data()`: Get historical synchronization data
- `get_sync_trends()`: Analyze synchronization trends
- `generate_sync_report()`: Generate comprehensive reports
- `export_analytics_to_json()`: Export analytics data

### Usage

```bash
# From command line
python -m receiver.sync_db.analytics report --days 30
python -m receiver.sync_db.analytics trends --days 7
python -m receiver.sync_db.analytics history
python -m receiver.sync_db.analytics export --days 30 --output sync_report.json
```

## Enhanced Downloader Service

### Features

1. **Improved Error Handling**: Better error categorization and handling
2. **Concurrent Downloads**: Efficient concurrent file downloading
3. **Detailed Statistics**: Comprehensive download statistics
4. **Enhanced Logging**: Structured logging with detailed information
5. **Progress Tracking**: Track download progress and performance

### Key Classes

#### EnhancedProtocolDownloader
Enhanced downloader service:
- `_download_single_file()`: Download individual files with error handling
- `_process_single_protocol()`: Process individual protocols
- `process_pending_protocols()`: Process all pending protocols

#### DownloadResult
Enhanced download result with detailed metrics:
- Basic download metrics
- File size and timing statistics
- File type distribution
- Domain-based success/failure tracking
- Error categorization

### Usage

```bash
# From command line
python -m receiver.downloader.enhanced_service --limit 100
python -m receiver.downloader.enhanced_service --output-dir /custom/path
```

## CLI Integration

### New Features in CLI Menu

1. **Enhanced Sync Options**: More flexible synchronization options including date ranges and periods
2. **Health Check Integration**: Built-in system health checking
3. **Infrastructure Validation**: Comprehensive infrastructure verification
4. **Improved Reporting**: Better formatted reports and statistics

### Menu Structure

```
=== ЗАГРУЗКА И ПОДГОТОВКА ДАННЫХ ===
1. Синхронизация протоколов из MongoDB
2. Скачивание протоколов за дату
3. Проверка доступности файлов в INPUT_DIR

=== СЛУЖЕБНЫЕ ФУНКЦИИ ===
25. Очистка тестовых данных
26. Создание тестовых файлов
27. Проверка инфраструктуры
```

### Usage Examples

#### Synchronization Options
```
1. Синхронизация протоколов из MongoDB
   ├── 1. Одна дата
   ├── 2. Диапазон дат
   ├── 3. Начальная дата + количество дней
   ├── 4. Ежедневное обновление
   └── 5. Полная синхронизация
```

#### Infrastructure Check
Menu option 27 provides comprehensive infrastructure validation:
- Directory permissions and existence
- Docker and container status
- VPN connectivity
- Disk space availability
- Python package validation
- Environment variable checking
- MongoDB connection testing

## Configuration

### Environment Variables

The enhanced components use the same environment variables as the original system:

```bash
# Remote MongoDB (for protocols)
MONGO_SERVER=192.168.0.46:8635
MONGO_USER=readProtocols223
MONGO_PASSWORD=your_password
MONGO_SSL_CERT=/path/to/certificate.crt
MONGO_PROTOCOLS_DB=protocols223
MONGO_PROTOCOLS_COLLECTION=purchaseProtocol

# Local MongoDB (for metadata)
MONGO_METADATA_SERVER=localhost:27018
LOCAL_MONGO_SERVER=localhost:27018
MONGO_METADATA_USER=admin
MONGO_METADATA_PASSWORD=your_password
MONGO_METADATA_DB=docling_metadata
```

## Installation

Install required dependencies:

```bash
pip install -r receiver/requirements.txt
```

## Testing

Run tests for individual components:

```bash
# Test sync service
python -m pytest tests/test_sync_service.py

# Test downloader
python -m pytest tests/test_downloader.py

# Test health checks
python -m pytest tests/test_health_checks.py
```

## Troubleshooting

### Common Issues

1. **MongoDB Connection Failures**
   - Verify environment variables are set correctly
   - Check SSL certificate path and permissions
   - Ensure VPN is connected for remote MongoDB access

2. **Download Failures**
   - Check zakupki.gov.ru accessibility
   - Verify network connectivity
   - Check disk space availability

3. **Performance Issues**
   - Adjust batch sizes in configuration
   - Tune concurrency settings
   - Monitor system resources

### Logs

Check logs for detailed error information:
- Enhanced services log to console with structured messages
- Enable debug logging with `--verbose` flag
- MongoDB operations are logged with timing information

## Future Enhancements

Planned improvements:
1. Web dashboard for monitoring synchronization status
2. Alerting system for failures and performance issues
3. Advanced analytics with predictive capabilities
4. Integration with external monitoring systems
5. Automated recovery mechanisms for common failures
