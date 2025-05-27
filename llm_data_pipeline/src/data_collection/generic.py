"""
Generic data collectors for CSV and JSONL formats.

This module provides generic collectors for CSV and JSONL data formats,
which can be used for sources other than CourtListener and Pile of Law.
"""

import csv
import gzip
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Generator, Optional, List, Union
import lzma

import pandas as pd
import requests

from .base import DataCollector, DataSourceConfig, Document


logger = logging.getLogger(__name__)


class CSVCollector(DataCollector):
    """Generic collector for CSV data files."""
    
    def __init__(self, config: DataSourceConfig):
        """Initialize the CSV collector.
        
        Args:
            config: Configuration for the CSV data source.
        """
        self.config = config
        self.connected = False
        self.metadata = {}
        
        # Validate configuration
        if not config.local_path:
            raise ValueError("local_path must be provided for CSV collector")
            
        # Set default metadata if not provided
        if not config.metadata:
            config.metadata = {}
            
        # Ensure required fields are specified
        if "text_field" not in config.metadata:
            raise ValueError("text_field must be specified in metadata")
        if "id_field" not in config.metadata:
            config.metadata["id_field"] = "id"  # Default ID field
            
    def connect(self) -> bool:
        """Check if the CSV file exists and is accessible.
        
        Returns:
            bool: True if file exists and is accessible, False otherwise.
        """
        try:
            if self.config.local_path.exists():
                self.connected = True
                # Extract metadata from file
                self.metadata["file_size"] = os.path.getsize(self.config.local_path)
                self.metadata["last_modified"] = os.path.getmtime(self.config.local_path)
                return True
            else:
                logger.error(f"CSV file not found: {self.config.local_path}")
                return False
        except Exception as e:
            logger.exception(f"Error connecting to CSV file: {e}")
            return False
            
    def collect(self, limit: Optional[int] = None) -> Generator[Document, None, None]:
        """Collect documents from the CSV file.
        
        Args:
            limit: Optional maximum number of documents to collect.
            
        Yields:
            Document: Documents collected from the CSV file.
            
        Raises:
            RuntimeError: If the CSV file is not accessible.
        """
        if not self.connected and not self.connect():
            raise RuntimeError(f"CSV file not accessible: {self.config.local_path}")
            
        # Get field mappings from configuration
        text_field = self.config.metadata["text_field"]
        id_field = self.config.metadata["id_field"]
        metadata_fields = self.config.metadata.get("metadata_fields", [])
        
        # Determine if file is gzipped
        is_gzipped = str(self.config.local_path).endswith(".gz")
        
        # Process the file in chunks to handle large files
        chunk_size = 10000  # Adjust based on memory constraints
        count = 0
        
        # Use pandas to read CSV in chunks
        open_func = gzip.open if is_gzipped else open
        
        try:
            with open_func(self.config.local_path, 'rt', encoding='utf-8') as f:
                # Process in chunks
                for chunk in pd.read_csv(
                    f, 
                    chunksize=chunk_size,
                    low_memory=False,
                    dtype=str,  # Treat all columns as strings initially
                    na_values=['', 'NULL', 'null', 'None', 'none'],
                    keep_default_na=True
                ):
                    # Process each row in the chunk
                    for _, row in chunk.iterrows():
                        # Skip rows with missing text
                        if text_field not in row or pd.isna(row[text_field]):
                            continue
                            
                        # Extract text and ID
                        text = row[text_field]
                        doc_id = row[id_field] if id_field in row and not pd.isna(row[id_field]) else f"unknown_{count}"
                        
                        # Build metadata dictionary
                        metadata = {}
                        for field in metadata_fields:
                            if field in row and not pd.isna(row[field]):
                                metadata[field] = row[field]
                                
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
                            
                    if limit and count >= limit:
                        break
        except Exception as e:
            logger.exception(f"Error processing CSV file: {e}")
            raise
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the CSV data source.
        
        Returns:
            Dict[str, Any]: Metadata about the data source.
        """
        if not self.connected and not self.connect():
            return {}
            
        return self.metadata


class JSONLCollector(DataCollector):
    """Generic collector for JSONL data files."""
    
    def __init__(self, config: DataSourceConfig):
        """Initialize the JSONL collector.
        
        Args:
            config: Configuration for the JSONL data source.
        """
        self.config = config
        self.connected = False
        self.metadata = {}
        
        # Validate configuration
        if not config.local_path:
            raise ValueError("local_path must be provided for JSONL collector")
            
        # Set default metadata if not provided
        if not config.metadata:
            config.metadata = {}
            
        # Ensure required fields are specified
        if "text_field" not in config.metadata:
            raise ValueError("text_field must be specified in metadata")
        if "id_field" not in config.metadata:
            config.metadata["id_field"] = "id"  # Default ID field
            
    def connect(self) -> bool:
        """Check if the JSONL file exists and is accessible.
        
        Returns:
            bool: True if file exists and is accessible, False otherwise.
        """
        try:
            if self.config.local_path.exists():
                self.connected = True
                # Extract metadata from file
                self.metadata["file_size"] = os.path.getsize(self.config.local_path)
                self.metadata["last_modified"] = os.path.getmtime(self.config.local_path)
                return True
            else:
                logger.error(f"JSONL file not found: {self.config.local_path}")
                return False
        except Exception as e:
            logger.exception(f"Error connecting to JSONL file: {e}")
            return False
            
    def collect(self, limit: Optional[int] = None) -> Generator[Document, None, None]:
        """Collect documents from the JSONL file.
        
        Args:
            limit: Optional maximum number of documents to collect.
            
        Yields:
            Document: Documents collected from the JSONL file.
            
        Raises:
            RuntimeError: If the JSONL file is not accessible.
        """
        if not self.connected and not self.connect():
            raise RuntimeError(f"JSONL file not accessible: {self.config.local_path}")
            
        # Get field mappings from configuration
        text_field = self.config.metadata["text_field"]
        id_field = self.config.metadata["id_field"]
        metadata_fields = self.config.metadata.get("metadata_fields", [])
        extract_all_metadata = not metadata_fields
        
        # Determine if file is compressed
        is_xz = str(self.config.local_path).endswith(".xz")
        is_gzipped = str(self.config.local_path).endswith(".gz")
        
        count = 0
        
        # Open file with appropriate method
        if is_xz:
            open_func = lzma.open
        elif is_gzipped:
            open_func = gzip.open
        else:
            open_func = open
        
        try:
            with open_func(self.config.local_path, 'rt', encoding='utf-8') as f:
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
        except Exception as e:
            logger.exception(f"Error processing JSONL file: {e}")
            raise
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the JSONL data source.
        
        Returns:
            Dict[str, Any]: Metadata about the data source.
        """
        if not self.connected and not self.connect():
            return {}
            
        return self.metadata
