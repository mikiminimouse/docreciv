#!/usr/bin/env python3
"""
Test script for enhanced preprocessing components.

This script verifies that the enhanced sync and downloader components
are working correctly.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_imports():
    """Test that all enhanced components can be imported."""
    print("Testing imports...")
    
    try:
        from docreciv.sync_db.enhanced_service import EnhancedSyncService, EnhancedSyncResult
        print("✅ EnhancedSyncService import successful")
    except ImportError as e:
        print(f"❌ EnhancedSyncService import failed: {e}")
        return False
    
    try:
        from docreciv.sync_db.health_checks import HealthCheckResult, check_vpn_connectivity
        print("✅ Health checks import successful")
    except ImportError as e:
        print(f"❌ Health checks import failed: {e}")
        return False
    
    try:
        from docreciv.sync_db.analytics import SyncAnalytics, SyncStatistics
        print("✅ Analytics import successful")
    except ImportError as e:
        print(f"❌ Analytics import failed: {e}")
        return False
    
    try:
        from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader, DownloadResult
        print("✅ EnhancedProtocolDownloader import successful")
    except ImportError as e:
        print(f"❌ EnhancedProtocolDownloader import failed: {e}")
        return False
    
    return True

def test_service_initialization():
    """Test that services can be initialized."""
    print("\nTesting service initialization...")
    
    try:
        from docreciv.sync_db.enhanced_service import EnhancedSyncService
        service = EnhancedSyncService()
        print("✅ EnhancedSyncService initialization successful")
        service.close()  # Clean up
    except Exception as e:
        print(f"❌ EnhancedSyncService initialization failed: {e}")
        return False
    
    try:
        from docreciv.downloader.enhanced_service import EnhancedProtocolDownloader
        downloader = EnhancedProtocolDownloader()
        print("✅ EnhancedProtocolDownloader initialization successful")
    except Exception as e:
        print(f"❌ EnhancedProtocolDownloader initialization failed: {e}")
        return False
    
    try:
        from docreciv.sync_db.analytics import SyncAnalytics
        analytics = SyncAnalytics()
        print("✅ SyncAnalytics initialization successful")
        analytics.close()  # Clean up
    except Exception as e:
        print(f"❌ SyncAnalytics initialization failed: {e}")
        return False
    
    return True

def test_data_structures():
    """Test that data structures work correctly."""
    print("\nTesting data structures...")
    
    try:
        from docreciv.sync_db.enhanced_service import EnhancedSyncResult
        result = EnhancedSyncResult(
            status="success",
            message="Test completed",
            date="2024-01-01"
        )
        print("✅ EnhancedSyncResult creation successful")
    except Exception as e:
        print(f"❌ EnhancedSyncResult creation failed: {e}")
        return False
    
    try:
        from docreciv.downloader.enhanced_service import DownloadResult
        result = DownloadResult(
            status="success",
            message="Test completed"
        )
        print("✅ DownloadResult creation successful")
    except Exception as e:
        print(f"❌ DownloadResult creation failed: {e}")
        return False
    
    return True

def main():
    """Main test function."""
    print("🧪 Testing Enhanced Preprocessing Components")
    print("=" * 50)
    
    # Run tests
    tests = [
        test_imports,
        test_service_initialization,
        test_data_structures
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        if test():
            passed += 1
        else:
            failed += 1
    
    # Summary
    print("\n" + "=" * 50)
    print("Test Summary:")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Total:  {passed + failed}")
    
    if failed == 0:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())