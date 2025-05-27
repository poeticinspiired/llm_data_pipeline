"""
Base classes and interfaces for data collection components.

This module defines the abstract base classes and interfaces that all data
collection components must implement, ensuring a consistent API across
different data sources.
"""

import abc
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union


class DataSourceType(Enum):
    """Enumeration of supported data source types."""
    COURT_LISTENER = "court_listener"
    PILE_OF_LAW = "pile_of_law"
    GENERIC_CSV = "generic_csv"
    GENERIC_JSONL = "generic_jsonl"


@dataclass
class DataSourceConfig:
    """Configuration for a data source."""
    source_type: DataSourceType
    name: str
    url: Optional[str] = None
    local_path: Optional[Path] = None
    metadata: Dict[str, Any] = None


@dataclass
class Document:
    """Represents a single document extracted from a data source."""
    id: str
    text: str
    metadata: Dict[str, Any]
    source: str
    source_id: str
    

class DataCollector(abc.ABC):
    """Abstract base class for all data collectors."""
    
    @abc.abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source.
        
        Returns:
            bool: True if connection was successful, False otherwise.
        """
        pass
    
    @abc.abstractmethod
    def collect(self, limit: Optional[int] = None) -> Generator[Document, None, None]:
        """Collect documents from the data source.
        
        Args:
            limit: Optional maximum number of documents to collect.
            
        Yields:
            Document: Documents collected from the source.
        """
        pass
    
    @abc.abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the data source.
        
        Returns:
            Dict[str, Any]: Metadata about the data source.
        """
        pass


class DataCollectorFactory:
    """Factory for creating data collectors based on configuration."""
    
    @staticmethod
    def create_collector(config: DataSourceConfig) -> DataCollector:
        """Create a data collector based on the provided configuration.
        
        Args:
            config: Configuration for the data source.
            
        Returns:
            DataCollector: An instance of a DataCollector implementation.
            
        Raises:
            ValueError: If the source type is not supported.
        """
        from .court_listener import CourtListenerCollector
        from .pile_of_law import PileOfLawCollector
        from .generic import CSVCollector, JSONLCollector
        
        if config.source_type == DataSourceType.COURT_LISTENER:
            return CourtListenerCollector(config)
        elif config.source_type == DataSourceType.PILE_OF_LAW:
            return PileOfLawCollector(config)
        elif config.source_type == DataSourceType.GENERIC_CSV:
            return CSVCollector(config)
        elif config.source_type == DataSourceType.GENERIC_JSONL:
            return JSONLCollector(config)
        else:
            raise ValueError(f"Unsupported data source type: {config.source_type}")
