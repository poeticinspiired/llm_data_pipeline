"""
CourtListener data collector implementation.

This module provides components for collecting legal text data from CourtListener's
bulk data files, which are provided in CSV format.
"""

import csv
import gzip
import io
import logging
import os
import tempfile
import urllib.request
from pathlib import Path
from typing import Any, Dict, Generator, Optional, List, Union

import pandas as pd
import requests

from .base import DataCollector, DataSourceConfig, Document


logger = logging.getLogger(__name__)


class CourtListenerCollector(DataCollector):
    """Collector for CourtListener bulk data files."""
    
    def __init__(self, config: DataSourceConfig):
        """Initialize the CourtListener collector.
        
        Args:
            config: Configuration for the CourtListener data source.
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
            
        # Default to opinions if not specified
        if "data_type" not in config.metadata:
            config.metadata["data_type"] = "opinions"
            
    def connect(self) -> bool:
        """Establish connection to the CourtListener data source.
        
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
        """Collect documents from the CourtListener data source.
        
        This method handles both remote and local files, and supports
        both plain CSV and gzipped CSV formats.
        
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
        """Process a CourtListener CSV file.
        
        This method handles both plain CSV and gzipped CSV formats.
        It uses pandas for efficient CSV processing, with chunking for
        large files.
        
        Args:
            file_path: Path to the CSV file.
            limit: Optional maximum number of documents to process.
            
        Yields:
            Document: Documents extracted from the CSV file.
        """
        # Determine if file is gzipped
        is_gzipped = str(file_path).endswith(".gz")
        
        # Determine the data type and set appropriate text and metadata fields
        data_type = self.config.metadata.get("data_type", "opinions")
        
        if data_type == "opinions":
            text_field = "plain_text"
            id_field = "id"
            metadata_fields = [
                "case_name", "date_filed", "docket_id", "court_id",
                "citation_count", "precedential_status"
            ]
        elif data_type == "dockets":
            text_field = "case_name"
            id_field = "id"
            metadata_fields = [
                "date_filed", "date_argued", "date_terminated",
                "court_id", "nature_of_suit", "cause"
            ]
        else:
            # Default mapping for other data types
            text_field = "text"
            id_field = "id"
            metadata_fields = []
            
        # Process the file in chunks to handle large files
        chunk_size = 10000  # Adjust based on memory constraints
        count = 0
        
        # Use pandas to read CSV in chunks
        open_func = gzip.open if is_gzipped else open
        
        with open_func(file_path, 'rt', encoding='utf-8') as f:
            # Read the header to get column names
            reader = csv.reader(f)
            header = next(reader)
            
            # Reset file pointer
            f.seek(0)
            
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
                        
                    # Extract text and metadata
                    text = row[text_field]
                    doc_id = row[id_field] if id_field in row else f"unknown_{count}"
                    
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
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get metadata about the CourtListener data source.
        
        Returns:
            Dict[str, Any]: Metadata about the data source.
        """
        if not self.connected and not self.connect():
            return {}
            
        return self.metadata
