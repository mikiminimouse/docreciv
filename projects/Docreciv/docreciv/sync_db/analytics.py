"""
Analytics module for SyncDB component.

Provides advanced statistics and analytics for protocol synchronization,
including historical data tracking, trend analysis, and reporting capabilities.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import json

from pymongo import MongoClient
from bson import ObjectId

from ..core.config import get_config

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler with formatting
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


@dataclass
class SyncStatistics:
    """Detailed statistics for a synchronization session."""
    # Basic metrics
    session_id: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    
    # Document processing metrics
    scanned_documents: int
    inserted_documents: int
    skipped_duplicates: int
    processing_errors: int
    
    # URL distribution
    single_url_documents: int
    multi_url_documents: int
    no_url_documents: int
    
    # Attachment analysis
    attachment_types: Dict[str, int]
    
    # Timing statistics
    average_processing_time: float
    max_processing_time: float
    min_processing_time: float
    
    # Error categorization
    error_types: Dict[str, int]
    
    # Metadata
    target_date: Optional[str] = None
    date_range: Optional[Tuple[str, str]] = None
    limit_applied: Optional[int] = None


@dataclass
class HistoricalSyncRecord:
    """Historical record of a synchronization session."""
    date: str  # YYYY-MM-DD
    total_scanned: int
    total_inserted: int
    total_skipped: int
    total_errors: int
    duration_seconds: float
    sync_type: str  # "daily", "range", "full"
    created_at: datetime


class SyncAnalytics:
    """Analytics service for protocol synchronization."""
    
    def __init__(self):
        """Initialize the analytics service."""
        self.config = get_config()
        self.client = None
        self.analytics_collection = None
        self.history_collection = None
        
    def _get_mongo_client(self) -> Optional[MongoClient]:
        """
        Get MongoDB client for analytics database.
        
        Returns:
            MongoClient or None if connection fails
        """
        if self.client:
            return self.client
            
        try:
            # Use local MongoDB configuration
            local_config = self.config.sync_db.local_mongo
            
            # Create connection URL
            url = local_config.get_connection_url()
            
            # Create client
            self.client = MongoClient(
                url,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=3000,
                socketTimeoutMS=5000,
                maxPoolSize=5
            )
            
            # Test connection
            self.client.admin.command("ping")
            
            # Get collections
            db = self.client[local_config.db]
            self.analytics_collection = db["sync_analytics"]
            self.history_collection = db["sync_history"]
            
            # Create indexes
            self._ensure_indexes()
            
            return self.client
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB for analytics: {e}")
            return None
    
    def _ensure_indexes(self) -> None:
        """Ensure required indexes exist."""
        try:
            if self.analytics_collection:
                self.analytics_collection.create_index([("session_id", 1)], name="session_id_idx")
                self.analytics_collection.create_index([("start_time", -1)], name="start_time_idx")
                self.analytics_collection.create_index([("target_date", 1)], name="target_date_idx")
            
            if self.history_collection:
                self.history_collection.create_index([("date", 1)], name="date_idx")
                self.history_collection.create_index([("sync_type", 1)], name="sync_type_idx")
                self.history_collection.create_index([("created_at", -1)], name="created_at_idx")
                
        except Exception as e:
            logger.warning(f"Failed to create indexes: {e}")
    
    def store_sync_statistics(self, stats: SyncStatistics) -> bool:
        """
        Store synchronization statistics in MongoDB.
        
        Args:
            stats: SyncStatistics object to store
            
        Returns:
            True if successful, False otherwise
        """
        try:
            client = self._get_mongo_client()
            if not client:
                return False
            
            # Convert to dictionary
            stats_dict = asdict(stats)
            
            # Add metadata
            stats_dict["_id"] = stats.session_id
            stats_dict["created_at"] = datetime.utcnow()
            
            # Store in analytics collection
            self.analytics_collection.replace_one(
                {"_id": stats.session_id},
                stats_dict,
                upsert=True
            )
            
            # Also store in history collection
            history_record = {
                "date": stats.target_date or (stats.date_range[0] if stats.date_range else datetime.now().strftime("%Y-%m-%d")),
                "total_scanned": stats.scanned_documents,
                "total_inserted": stats.inserted_documents,
                "total_skipped": stats.skipped_duplicates,
                "total_errors": stats.processing_errors,
                "duration_seconds": stats.duration_seconds,
                "sync_type": "range" if stats.date_range else "daily",
                "created_at": datetime.utcnow()
            }
            
            self.history_collection.insert_one(history_record)
            
            logger.info(f"Stored sync statistics for session {stats.session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store sync statistics: {e}")
            return False
    
    def get_sync_statistics(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve synchronization statistics by session ID.
        
        Args:
            session_id: Session ID to retrieve
            
        Returns:
            Dictionary with statistics or None if not found
        """
        try:
            client = self._get_mongo_client()
            if not client:
                return None
            
            result = self.analytics_collection.find_one({"_id": session_id})
            if result:
                # Remove MongoDB-specific fields
                result.pop("_id", None)
                result.pop("created_at", None)
                return result
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve sync statistics: {e}")
            return None
    
    def get_historical_sync_data(
        self,
        days: int = 30,
        sync_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get historical synchronization data.
        
        Args:
            days: Number of days of history to retrieve
            sync_type: Filter by sync type ("daily", "range", "full")
            
        Returns:
            List of historical records
        """
        try:
            client = self._get_mongo_client()
            if not client:
                return []
            
            # Calculate date threshold
            threshold_date = datetime.utcnow() - timedelta(days=days)
            
            # Build query
            query = {"created_at": {"$gte": threshold_date}}
            if sync_type:
                query["sync_type"] = sync_type
            
            # Retrieve records
            cursor = self.history_collection.find(
                query,
                sort=[("created_at", -1)]
            )
            
            records = []
            for record in cursor:
                # Remove MongoDB-specific fields
                record.pop("_id", None)
                records.append(record)
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to retrieve historical sync data: {e}")
            return []
    
    def get_sync_trends(self, days: int = 30) -> Dict[str, Any]:
        """
        Analyze synchronization trends over time.
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Dictionary with trend analysis
        """
        try:
            records = self.get_historical_sync_data(days)
            if not records:
                return {}
            
            # Используем функцию агрегации из metrics_aggregator
            from docreciv.webui.utils.metrics_aggregator import aggregate_by_days_simple
            
            # Агрегируем данные по дням
            daily_averages = aggregate_by_days_simple(records, days=None)  # Фильтрация уже сделана в get_historical_sync_data
            
            # Формируем результат в том же формате, что и раньше
            trends = {
                "daily_averages": daily_averages,
                "total_records": len(records),
                "date_range": {
                    "start": min(records, key=lambda x: x.get("created_at", datetime.min)).get("created_at"),
                    "end": max(records, key=lambda x: x.get("created_at", datetime.min)).get("created_at")
                }
            }
            
            return trends
            
        except Exception as e:
            logger.error(f"Failed to analyze sync trends: {e}")
            return {}
    
    def generate_sync_report(self, days: int = 30) -> Dict[str, Any]:
        """
        Generate a comprehensive synchronization report.
        
        Args:
            days: Number of days to include in report
            
        Returns:
            Dictionary with report data
        """
        try:
            records = self.get_historical_sync_data(days)
            if not records:
                return {"error": "No historical data available"}
            
            # Calculate totals
            total_scanned = sum(r.get("total_scanned", 0) for r in records)
            total_inserted = sum(r.get("total_inserted", 0) for r in records)
            total_skipped = sum(r.get("total_skipped", 0) for r in records)
            total_errors = sum(r.get("total_errors", 0) for r in records)
            total_duration = sum(r.get("duration_seconds", 0) for r in records)
            
            # Calculate averages
            avg_scanned = total_scanned / len(records) if records else 0
            avg_inserted = total_inserted / len(records) if records else 0
            avg_duration = total_duration / len(records) if records else 0
            error_rate = (total_errors / total_scanned * 100) if total_scanned > 0 else 0
            
            # Get recent trends
            trends = self.get_sync_trends(7)  # Last week
            
            report = {
                "summary": {
                    "period_days": days,
                    "total_sessions": len(records),
                    "total_scanned": total_scanned,
                    "total_inserted": total_inserted,
                    "total_skipped": total_skipped,
                    "total_errors": total_errors,
                    "average_scanned_per_session": round(avg_scanned, 2),
                    "average_inserted_per_session": round(avg_inserted, 2),
                    "average_duration_seconds": round(avg_duration, 2),
                    "error_rate_percent": round(error_rate, 2)
                },
                "recent_trends": trends,
                "top_sessions": sorted(records, key=lambda x: x.get("total_inserted", 0), reverse=True)[:5]
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate sync report: {e}")
            return {"error": f"Failed to generate report: {e}"}
    
    def export_analytics_to_json(self, days: int = 30, filepath: Optional[str] = None) -> bool:
        """
        Export analytics data to JSON file.
        
        Args:
            days: Number of days to export
            filepath: Output file path (defaults to sync_analytics_export.json)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not filepath:
                filepath = f"sync_analytics_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Get data
            report = self.generate_sync_report(days)
            
            # Write to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"Exported analytics data to {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export analytics data: {e}")
            return False
    
    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.debug("Closed MongoDB connection for analytics")


def main():
    """Entry point for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Analytics for SyncDB component")
    parser.add_argument(
        "command",
        choices=["report", "trends", "history", "export"],
        help="Command to execute"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to analyze (default: 30)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path for export"
    )
    parser.add_argument(
        "--session-id",
        type=str,
        help="Specific session ID to retrieve"
    )
    
    args = parser.parse_args()
    
    # Create analytics service
    analytics = SyncAnalytics()
    
    try:
        if args.command == "report":
            report = analytics.generate_sync_report(args.days)
            print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
            
        elif args.command == "trends":
            trends = analytics.get_sync_trends(args.days)
            print(json.dumps(trends, ensure_ascii=False, indent=2, default=str))
            
        elif args.command == "history":
            if args.session_id:
                stats = analytics.get_sync_statistics(args.session_id)
                if stats:
                    print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
                else:
                    print(f"No statistics found for session {args.session_id}")
                    return 1
            else:
                history = analytics.get_historical_sync_data(args.days)
                print(json.dumps(history, ensure_ascii=False, indent=2, default=str))
                
        elif args.command == "export":
            success = analytics.export_analytics_to_json(args.days, args.output)
            if success:
                print(f"Successfully exported analytics to {args.output or 'default file'}")
            else:
                print("Failed to export analytics")
                return 1
                
        return 0
        
    finally:
        analytics.close()


if __name__ == "__main__":
    exit(main())
