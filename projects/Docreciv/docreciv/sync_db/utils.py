"""
Utility functions for the Sync DB microservice.
"""
import uuid
from typing import List, Dict, Any
from datetime import datetime


def generate_unit_id() -> str:
    """Generate a unique unit ID in the format UNIT_<16hex>."""
    return f"UNIT_{uuid.uuid4().hex[:16]}"


def extract_urls_from_attachments(raw_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract URLs from attachments field with support for different structures."""
    urls = []

    # Handle different attachment structures
    attachments = raw_doc.get("attachments")

    if isinstance(attachments, dict):
        docs_field = attachments.get("document", [])
        if isinstance(docs_field, dict):
            docs_field = [docs_field]
        if isinstance(docs_field, list):
            for item in docs_field:
                if isinstance(item, dict):
                    add_from_doc(item)
    elif isinstance(attachments, list):
        for item in attachments:
            if isinstance(item, dict):
                add_from_doc(item)

    def add_from_doc(doc: Dict[str, Any]) -> None:
        url = doc.get("url") or doc.get("downloadUrl") or doc.get("fileUrl")
        if url:
            urls.append({
                "url": url,
                "fileName": doc.get("fileName") or doc.get("name") or "",
                "guid": doc.get("guid"),
                "contentUid": doc.get("contentUid"),
                "description": doc.get("description"),
            })

    return urls


def create_protocol_document(raw_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Create a protocol document for insertion into local MongoDB."""
    # Extract purchase notice number
    purchase_info = raw_doc.get("purchaseInfo", {})
    pn = purchase_info.get("purchaseNoticeNumber") if isinstance(purchase_info, dict) else None
    
    # Extract URLs from attachments
    urls = extract_urls_from_attachments(raw_doc)
    
    # Get load date
    load_date = raw_doc.get("loadDate")
    now_ts = datetime.utcnow()
    
    # Create document with all required fields
    doc_to_insert = {
        # Service fields for preprocessing
        "unit_id": generate_unit_id(),
        "urls": urls,
        "multi_url": len(urls) > 1,
        "url_count": len(urls),
        "source": "remote_mongo_direct",
        "status": "pending",
        "created_at": now_ts,
        "updated_at": now_ts,
        
        # FULL PROTOCOL DATA FROM MONGODB
        **raw_doc  # Include ALL fields from original document
    }
    
    return doc_to_insert
