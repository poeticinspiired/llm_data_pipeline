"""
Main entry point for the data storage module.

This module provides a simple interface for storing documents in MongoDB
and raw data in cloud storage solutions.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

from ..data_processing.base import ProcessedDocument
from .mongodb import MongoDBStorage
from .cloud import S3Storage, LocalStorage


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_mongodb_storage(
    connection_string: str,
    database_name: str,
    collection_name: str
) -> MongoDBStorage:
    """Create a MongoDB storage instance.
    
    Args:
        connection_string: MongoDB connection string.
        database_name: Name of the database to use.
        collection_name: Name of the collection to use.
        
    Returns:
        MongoDBStorage: MongoDB storage instance.
    """
    storage = MongoDBStorage(
        connection_string=connection_string,
        database_name=database_name,
        collection_name=collection_name,
        create_indexes=True
    )
    
    # Test connection
    if not storage.connect():
        logger.warning(f"Failed to connect to MongoDB: {connection_string}")
        
    return storage


def create_s3_storage(
    bucket_name: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: Optional[str] = None
) -> S3Storage:
    """Create an S3 storage instance.
    
    Args:
        bucket_name: Name of the S3 bucket.
        aws_access_key_id: Optional AWS access key ID.
        aws_secret_access_key: Optional AWS secret access key.
        region_name: Optional AWS region name.
        
    Returns:
        S3Storage: S3 storage instance.
    """
    storage = S3Storage(
        bucket_name=bucket_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    
    # Test connection
    if not storage.connect():
        logger.warning(f"Failed to connect to S3 bucket: {bucket_name}")
        
    return storage


def create_local_storage(base_dir: Union[str, Path]) -> LocalStorage:
    """Create a local storage instance.
    
    Args:
        base_dir: Base directory for storage.
        
    Returns:
        LocalStorage: Local storage instance.
    """
    storage = LocalStorage(base_dir=base_dir)
    
    # Test connection
    if not storage.connect():
        logger.warning(f"Failed to connect to local storage: {base_dir}")
        
    return storage


def store_processed_documents(
    storage: MongoDBStorage,
    documents: List[ProcessedDocument]
) -> Dict[str, bool]:
    """Store processed documents in MongoDB.
    
    Args:
        storage: MongoDB storage instance.
        documents: List of processed documents to store.
        
    Returns:
        Dict[str, bool]: Dictionary mapping document IDs to storage success.
    """
    logger.info(f"Storing {len(documents)} processed documents in MongoDB")
    return storage.store_documents(documents)


def store_raw_data(
    storage: Union[S3Storage, LocalStorage],
    data: Union[str, bytes, Path],
    remote_path: str,
    metadata: Optional[Dict[str, str]] = None
) -> bool:
    """Store raw data in cloud storage.
    
    Args:
        storage: Cloud storage instance.
        data: Data to store (string, bytes, or file path).
        remote_path: Path in cloud storage.
        metadata: Optional metadata to store with the data.
        
    Returns:
        bool: True if storage was successful, False otherwise.
    """
    logger.info(f"Storing raw data in cloud storage: {remote_path}")
    
    # Handle different data types
    if isinstance(data, str):
        if Path(data).exists():
            # It's a file path
            return storage.store_file(data, remote_path, metadata)
        else:
            # It's a string
            return storage.store_text(data, remote_path, metadata)
    elif isinstance(data, bytes):
        # Store bytes in a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(data)
            temp_path = temp_file.name
            
        # Store the temporary file
        result = storage.store_file(temp_path, remote_path, metadata)
        
        # Clean up
        import os
        os.unlink(temp_path)
        
        return result
    elif isinstance(data, Path):
        # It's a file path
        return storage.store_file(data, remote_path, metadata)
    else:
        logger.error(f"Unsupported data type: {type(data)}")
        return False
