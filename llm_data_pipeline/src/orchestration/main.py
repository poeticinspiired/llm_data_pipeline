"""
Main entry point for the orchestration module.

This module provides a simple interface for running the pipeline
with different configurations.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from .prefect_flow import llm_data_pipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def run_pipeline(
    config_path: Optional[str] = None,
    source_type: Optional[str] = None,
    source_name: Optional[str] = None,
    source_config: Optional[Dict] = None,
    limit: Optional[int] = None,
    dataset_version: Optional[str] = None
) -> Dict:
    """Run the LLM data pipeline with the specified configuration.
    
    Args:
        config_path: Path to a JSON configuration file.
        source_type: Type of data source (overrides config file).
        source_name: Name of the data source (overrides config file).
        source_config: Configuration for the data source (overrides config file).
        limit: Maximum number of documents to collect.
        dataset_version: Dataset version name.
        
    Returns:
        Dict: Pipeline results.
    """
    # Load configuration from file if provided
    if config_path:
        import json
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        # Use default configuration
        config = {
            "source": {
                "type": source_type or "court_listener",
                "name": source_name or "default",
                "config": source_config or {}
            },
            "processing": {
                "use_default_pipeline": True,
                "batch_size": 100
            },
            "storage": {
                "mongodb": {
                    "connection_string": "mongodb://localhost:27017/",
                    "database_name": "llm_data_pipeline",
                    "collection_name": "processed_documents"
                },
                "raw_storage": {
                    "type": "local",
                    "base_dir": "data/raw"
                }
            }
        }
    
    # Override configuration with provided parameters
    if source_type:
        config["source"]["type"] = source_type
    if source_name:
        config["source"]["name"] = source_name
    if source_config:
        config["source"]["config"] = source_config
    
    # Generate dataset version if not provided
    if not dataset_version:
        dataset_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Run the pipeline
    logger.info(f"Running pipeline with configuration: {config}")
    results = llm_data_pipeline(
        source_config=config["source"],
        processing_config=config["processing"],
        storage_config=config["storage"],
        limit=limit,
        dataset_version=dataset_version
    )
    
    return results


def run_court_listener_pipeline(
    bulk_dir: str,
    limit: Optional[int] = None,
    dataset_version: Optional[str] = None
) -> Dict:
    """Run the pipeline with CourtListener data.
    
    Args:
        bulk_dir: Directory containing CourtListener bulk data.
        limit: Maximum number of documents to collect.
        dataset_version: Dataset version name.
        
    Returns:
        Dict: Pipeline results.
    """
    source_config = {
        "bulk_dir": bulk_dir
    }
    
    return run_pipeline(
        source_type="court_listener",
        source_name="court_listener_bulk",
        source_config=source_config,
        limit=limit,
        dataset_version=dataset_version
    )


def run_pile_of_law_pipeline(
    jsonl_path: str,
    limit: Optional[int] = None,
    dataset_version: Optional[str] = None
) -> Dict:
    """Run the pipeline with Pile of Law data.
    
    Args:
        jsonl_path: Path to Pile of Law JSONL file.
        limit: Maximum number of documents to collect.
        dataset_version: Dataset version name.
        
    Returns:
        Dict: Pipeline results.
    """
    source_config = {
        "local_path": jsonl_path
    }
    
    return run_pipeline(
        source_type="pile_of_law",
        source_name="pile_of_law_local",
        source_config=source_config,
        limit=limit,
        dataset_version=dataset_version
    )


def run_generic_jsonl_pipeline(
    jsonl_path: str,
    text_field: str = "text",
    id_field: str = "id",
    limit: Optional[int] = None,
    dataset_version: Optional[str] = None
) -> Dict:
    """Run the pipeline with generic JSONL data.
    
    Args:
        jsonl_path: Path to JSONL file.
        text_field: Field containing the document text.
        id_field: Field containing the document ID.
        limit: Maximum number of documents to collect.
        dataset_version: Dataset version name.
        
    Returns:
        Dict: Pipeline results.
    """
    source_config = {
        "local_path": jsonl_path,
        "metadata": {
            "text_field": text_field,
            "id_field": id_field
        }
    }
    
    return run_pipeline(
        source_type="generic_jsonl",
        source_name=f"generic_jsonl_{Path(jsonl_path).stem}",
        source_config=source_config,
        limit=limit,
        dataset_version=dataset_version
    )
