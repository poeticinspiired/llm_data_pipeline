"""
Main entry point for the data collection module.

This module provides a simple interface for collecting data from various sources.
"""

import logging
from pathlib import Path
from typing import Dict, Generator, List, Optional, Union

from .base import DataCollector, DataSourceConfig, DataSourceType, Document
from .court_listener import CourtListenerCollector
from .pile_of_law import PileOfLawCollector
from .generic import CSVCollector, JSONLCollector


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_collector(
    source_type: str,
    name: str,
    url: Optional[str] = None,
    local_path: Optional[Union[str, Path]] = None,
    metadata: Optional[Dict] = None
) -> DataCollector:
    """Create a data collector for the specified source.
    
    Args:
        source_type: Type of data source (e.g., "court_listener", "pile_of_law").
        name: Name of the data source.
        url: Optional URL for remote data sources.
        local_path: Optional local path for file-based data sources.
        metadata: Optional metadata for configuring the collector.
        
    Returns:
        DataCollector: An instance of a DataCollector implementation.
        
    Raises:
        ValueError: If the source type is not supported.
    """
    # Convert string path to Path object if provided
    if local_path and isinstance(local_path, str):
        local_path = Path(local_path)
        
    # Create configuration
    config = DataSourceConfig(
        source_type=DataSourceType(source_type),
        name=name,
        url=url,
        local_path=local_path,
        metadata=metadata or {}
    )
    
    # Create collector using factory
    from .base import DataCollectorFactory
    return DataCollectorFactory.create_collector(config)


def collect_documents(
    collector: DataCollector,
    limit: Optional[int] = None
) -> Generator[Document, None, None]:
    """Collect documents from the specified collector.
    
    Args:
        collector: Data collector to use.
        limit: Optional maximum number of documents to collect.
        
    Yields:
        Document: Documents collected from the source.
        
    Raises:
        RuntimeError: If the collector fails to connect or collect data.
    """
    # Ensure collector is connected
    if not collector.connect():
        raise RuntimeError(f"Failed to connect to data source: {collector.config.name}")
        
    # Collect documents
    logger.info(f"Collecting documents from {collector.config.name}")
    count = 0
    
    for doc in collector.collect(limit=limit):
        yield doc
        count += 1
        
        # Log progress periodically
        if count % 1000 == 0:
            logger.info(f"Collected {count} documents from {collector.config.name}")
            
    logger.info(f"Finished collecting {count} documents from {collector.config.name}")


def collect_sample(
    collector: DataCollector,
    sample_size: int = 10
) -> List[Document]:
    """Collect a sample of documents from the specified collector.
    
    This is useful for testing and validation.
    
    Args:
        collector: Data collector to use.
        sample_size: Number of documents to collect.
        
    Returns:
        List[Document]: Sample documents collected from the source.
        
    Raises:
        RuntimeError: If the collector fails to connect or collect data.
    """
    return list(collect_documents(collector, limit=sample_size))
