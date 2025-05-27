"""
Pile of Law data collector implementation.

This module provides components for collecting legal text data from the Pile of Law
dataset, which is provided in JSONL format (often compressed with XZ).
"""

import json
import logging
import os
import tempfile
import urllib.request
from pathlib import Path
from typing import Any, Dict, Generator, Optional, List, Union
import lzma

import requests

from .base import DataCollector, DataSourceConfig, Document


logger = logging.getLogger(__name__)


class PileOfLawCollector(DataCollector):
    """Collector for Pile of Law dataset files."""
    
    def __init__(self, config: DataSourceConfig):
        """Initialize the Pile of Law collector.
        
        Args:
            config: Configuration for the Pile of Law data source.
        """
        self.config = config
        self.connected = False
        self.metadata = {}
        
        # Validate configuration
        if not config.url and not config.local_path:
            raise ValueError("Either url or local_path must be provided")
            
        # Set default metadata if not provided
        if not config.metadata:
            config.metadata = {}
            
    def connect(self) -> bool:
        """Establish connection to the Pile of Law data source.
        
        For remote files, this checks if the URL is accessible.
        For local files, this checks if the file exists.
        
        Returns:
            bool: True if connection was successful, False otherwise.
        """
        try:
            if self.config.url:
                # For remote files, check if URL is accessible
                response = requests.head(self.config.url, timeout=10)
                if response.status_code == 200:
                    self.connected = True
                    # Extract metadata from headers
                    self.metadata["content_length"] = response.headers.get("Content-Length")
                    self.metadata["last_modified"] = response.headers.get("Last-Modified")
                    return True
                else:
                    logger.error(f"Failed to connect to {self.config.url}: {response.status_code}")
                    return False
            elif self.config.local_path:
                # For local files, check if file exists
                if self.config.local_path.exists():
                    self.connected = True
                    # Extract metadata from file
                    self.metadata["file_size"] = os.path.getsize(self.config.local_path)
                    self.metadata["last_modified"] = os.path.getmtime(self.config.local_path)
                    return True
                else:
                    logger.error(f"Local file not found: {self.config.local_path}")
                    return False
            else:
                logger.error("Neither URL nor local path provided")
                return False
        except Exception as e:
            logger.exception(f"Error connecting to data source: {e}")
            return False
            
    def collect(self, limit: Optional[int] = None) -> Generator[Document, None, None]:
        """Collect documents from the Pile of Law data source.
        
        This method handles both remote and local files, and supports
        both plain JSONL and XZ-compressed JSONL formats.
        
        Args:
            limit: Optional maximum number of documents to collect.
            
        Yields:
            Document: Documents collected from the source.
            
        Raises:
            RuntimeError: If not connected to the data source.
        """
        if not self.connected and not self.connect():
            raise RuntimeError("Not connected to data source")
            
        count = 0
        
        try:
            # Handle remote files
            if self.config.url:
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    logger.info(f"Downloading {self.config.url} to {temp_file.name}")
                    urllib.request.urlretrieve(self.config.url, temp_file.name)
                    
                    # Process the downloaded file
                    for doc in self._process_file(Path(temp_file.name), limit):
                        yield doc
                        count += 1
                        if limit and count >= limit:
                            break
                            
                    # Clean up
                    os.unlink(temp_file.name)
            
            # Handle local files
            elif self.config.local_path:
                for doc in self._process_file(self.config.local_path, limit):
                    yield doc
                    count += 1
                    if limit and count >= limit:
                        break
        
        except Exception as e:
            logger.exception(f"Error collecting data: {e}")
            raise
            
    def _process_file(self, file_path: Path, limit: Optional[int] = None) -> Generator[Document, None, None]:
        """Process a Pile of Law JSONL file.
        
        This method handles both plain JSONL and XZ-compressed JSONL formats.
        It processes the file line by line to minimize memory usage.
        
        Args:
            file_path: Path to the JSONL file.
            limit: Optional maximum number of documents to process.
            
        Yields:
            Document: Documents extracted from the JSONL file.
        """
        # Determine if file is XZ-compressed
        is_xz = str(file_path).endswith(".xz")
        
        # Set field mappings based on configuration or defaults
        text_field = self.config.metadata.get("text_field", "text")
        id_field = self.config.metadata.get("id_field", "id")
        
        # Get list of metadata fields to extract, or extract all if not specified
        metadata_fields = self.config.metadata.get("metadata_fields", [])
        extract_all_metadata = not metadata_fields
        
        count = 0
        
        # Open file with appropriate method
        open_func = lzma.open if is_xz else open
        
        with open_func(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                # Skip empty lines
                if not line.strip():
                    continue
                    
                try:
                    # Parse JSON
                    data = json.loads(line)
                    
                    # Skip entries without text
                    if text_field not in data or not data[text_field]:
                        continue
                        
                    # Extract text
                    text = data[text_field]
                    
                    # Extract ID
                    doc_id = data.get(id_field, f"unknown_{count}")
                    
                    # Extract metadata
                    metadata = {}
                    if extract_all_metadata:
                        # Extract all fields except text field
                        metadata = {k: v for k, v in data.items() if k != text_field}
                    else:
                        # Extract only specified metadata fields
                        for field in metadata_fields:
                            if field in data:
                                metadata[field] = data[field]
                                
                    # Create document
                    doc = Document(
                        id=str(doc_id),
                        text=text,
                        metadata=metadata,
                        source=self.config.name,
                        source_id=str(doc_id)
                    )
                    
                    yield doc
                    
                    count += 1
                    if limit and count >= limit:
                        break
                        
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse JSON: {line[:100]}...")
                    continue
                except Exception as e:
                    logger.warning(f"Error processing line: {e}")
                    continue
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the Pile of Law data source.
        
        Returns:
            Dict[str, Any]: Metadata about the data source.
        """
        if not self.connected and not self.connect():
            return {}
            
        return self.metadata
