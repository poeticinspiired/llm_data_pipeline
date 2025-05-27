"""
Prefect-based orchestration for the LLM Training Data Curation Pipeline.

This module provides a Prefect-based implementation for orchestrating
the entire pipeline, from data collection to storage.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from prefect import flow, task
from prefect.logging import get_run_logger

from ..data_collection.main import create_collector, collect_documents
from ..data_processing.main import create_default_pipeline, process_documents
from ..data_storage.main import (
    create_mongodb_storage,
    create_local_storage,
    create_s3_storage,
    store_processed_documents,
    store_raw_data
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


@task(name="collect_data_task")
def collect_data_task(
    source_type: str,
    source_name: str,
    source_config: Dict[str, Any],
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Collect data from a source.
    
    Args:
        source_type: Type of data source.
        source_name: Name of the data source.
        source_config: Configuration for the data source.
        limit: Optional maximum number of documents to collect.
        
    Returns:
        List[Dict[str, Any]]: Collected documents as dictionaries.
    """
    task_logger = get_run_logger()
    task_logger.info(f"Collecting data from {source_name} ({source_type})")
    
    # Create collector
    collector = create_collector(
        source_type=source_type,
        name=source_name,
        **source_config
    )
    
    # Collect documents
    documents = list(collect_documents(collector, limit=limit))
    
    task_logger.info(f"Collected {len(documents)} documents from {source_name}")
    
    # Convert to dictionaries for serialization
    doc_dicts = []
    for doc in documents:
        doc_dicts.append({
            "id": doc.id,
            "text": doc.text,
            "metadata": doc.metadata,
            "source": doc.source,
            "source_id": doc.source_id
        })
    
    return doc_dicts


@task(name="process_data_task")
def process_data_task(
    documents: List[Dict[str, Any]],
    use_default_pipeline: bool = True,
    pipeline_config: Optional[Dict[str, Any]] = None,
    batch_size: int = 100
) -> List[Dict[str, Any]]:
    """Process documents through the processing pipeline.
    
    Args:
        documents: Documents to process.
        use_default_pipeline: Whether to use the default pipeline.
        pipeline_config: Configuration for a custom pipeline.
        batch_size: Size of batches for processing.
        
    Returns:
        List[Dict[str, Any]]: Processed documents as dictionaries.
    """
    from ..data_collection.base import Document
    
    task_logger = get_run_logger()
    task_logger.info(f"Processing {len(documents)} documents")
    
    # Convert dictionaries back to Document objects
    doc_objects = []
    for doc_dict in documents:
        doc = Document(
            id=doc_dict["id"],
            text=doc_dict["text"],
            metadata=doc_dict["metadata"],
            source=doc_dict["source"],
            source_id=doc_dict["source_id"]
        )
        doc_objects.append(doc)
    
    # Create pipeline
    if use_default_pipeline:
        pipeline = create_default_pipeline()
    else:
        # Custom pipeline configuration would be implemented here
        pipeline = create_default_pipeline()
    
    # Process documents
    processed_docs = list(process_documents(
        documents=doc_objects,
        pipeline=pipeline,
        batch_size=batch_size
    ))
    
    task_logger.info(f"Processed {len(processed_docs)} documents")
    
    # Convert to dictionaries for serialization
    processed_dicts = []
    for doc in processed_docs:
        processed_dicts.append({
            "id": doc.id,
            "text": doc.text,
            "tokens": doc.tokens,
            "token_count": doc.token_count,
            "quality_score": doc.quality_score,
            "quality_metrics": doc.quality_metrics,
            "source": doc.source,
            "source_id": doc.source_id,
            "original_metadata": doc.original_metadata,
            "enhanced_metadata": doc.enhanced_metadata,
            "processing_history": doc.processing_history,
            "processing_metadata": doc.processing_metadata
        })
    
    return processed_dicts


@task(name="store_data_task")
def store_data_task(
    processed_documents: List[Dict[str, Any]],
    mongodb_config: Dict[str, Any],
    raw_storage_config: Dict[str, Any],
    dataset_version: Optional[str] = None
) -> Dict[str, Any]:
    """Store processed documents in MongoDB and raw data in cloud storage.
    
    Args:
        processed_documents: Processed documents to store.
        mongodb_config: Configuration for MongoDB storage.
        raw_storage_config: Configuration for raw data storage.
        dataset_version: Optional dataset version name.
        
    Returns:
        Dict[str, Any]: Storage results.
    """
    from ..data_processing.base import ProcessedDocument
    
    task_logger = get_run_logger()
    task_logger.info(f"Storing {len(processed_documents)} documents")
    
    # Convert dictionaries back to ProcessedDocument objects
    doc_objects = []
    for doc_dict in processed_documents:
        doc = ProcessedDocument(
            id=doc_dict["id"],
            source=doc_dict["source"],
            source_id=doc_dict["source_id"],
            text=doc_dict["text"]
        )
        doc.tokens = doc_dict.get("tokens")
        doc.token_count = doc_dict.get("token_count")
        doc.quality_score = doc_dict.get("quality_score")
        doc.quality_metrics = doc_dict.get("quality_metrics", {})
        doc.original_metadata = doc_dict.get("original_metadata", {})
        doc.enhanced_metadata = doc_dict.get("enhanced_metadata", {})
        doc.processing_history = doc_dict.get("processing_history", [])
        doc.processing_metadata = doc_dict.get("processing_metadata", {})
        doc_objects.append(doc)
    
    # Create MongoDB storage
    mongodb_storage = create_mongodb_storage(
        connection_string=mongodb_config["connection_string"],
        database_name=mongodb_config["database_name"],
        collection_name=mongodb_config["collection_name"]
    )
    
    # Store documents in MongoDB
    mongodb_results = store_processed_documents(mongodb_storage, doc_objects)
    
    # Create raw storage
    if raw_storage_config["type"] == "s3":
        raw_storage = create_s3_storage(
            bucket_name=raw_storage_config["bucket_name"],
            aws_access_key_id=raw_storage_config.get("aws_access_key_id"),
            aws_secret_access_key=raw_storage_config.get("aws_secret_access_key"),
            region_name=raw_storage_config.get("region_name")
        )
    else:
        # Default to local storage
        raw_storage = create_local_storage(
            base_dir=raw_storage_config["base_dir"]
        )
    
    # Store raw data
    raw_results = {}
    for doc in doc_objects:
        # Skip documents that failed MongoDB storage
        if not mongodb_results.get(doc.id, False):
            continue
            
        # Store raw text
        remote_path = f"raw/{doc.source}/{doc.id}.txt"
        metadata = {
            "source": doc.source,
            "source_id": doc.source_id,
            "stored_at": datetime.utcnow().isoformat(),
            "processed": True
        }
        
        raw_results[doc.id] = raw_storage.store_text(doc.text, remote_path, metadata)
    
    # Create dataset version if requested
    version_result = None
    if dataset_version:
        version_count = mongodb_storage.create_dataset_version(dataset_version)
        version_result = {
            "version_name": dataset_version,
            "document_count": version_count
        }
    
    # Compile results
    results = {
        "mongodb": {
            "success_count": sum(1 for success in mongodb_results.values() if success),
            "total_count": len(mongodb_results)
        },
        "raw_storage": {
            "success_count": sum(1 for success in raw_results.values() if success),
            "total_count": len(raw_results)
        },
        "dataset_version": version_result
    }
    
    task_logger.info(f"Storage results: {results}")
    return results


@flow(
    name="llm_data_pipeline",
    description="LLM Training Data Curation Pipeline"
)
def llm_data_pipeline(
    source_config: Dict[str, Any],
    processing_config: Dict[str, Any],
    storage_config: Dict[str, Any],
    limit: Optional[int] = None,
    dataset_version: Optional[str] = None
) -> Dict[str, Any]:
    """Run the complete LLM training data curation pipeline.
    
    Args:
        source_config: Configuration for the data source.
        processing_config: Configuration for data processing.
        storage_config: Configuration for data storage.
        limit: Optional maximum number of documents to collect.
        dataset_version: Optional dataset version name.
        
    Returns:
        Dict[str, Any]: Pipeline results.
    """
    flow_logger = get_run_logger()
    flow_logger.info("Starting LLM data pipeline")
    
    # Step 1: Collect data
    documents = collect_data_task(
        source_type=source_config["type"],
        source_name=source_config["name"],
        source_config=source_config["config"],
        limit=limit
    )
    
    # Step 2: Process data
    processed_documents = process_data_task(
        documents=documents,
        use_default_pipeline=processing_config.get("use_default_pipeline", True),
        pipeline_config=processing_config.get("pipeline_config"),
        batch_size=processing_config.get("batch_size", 100)
    )
    
    # Step 3: Store data
    storage_results = store_data_task(
        processed_documents=processed_documents,
        mongodb_config=storage_config["mongodb"],
        raw_storage_config=storage_config["raw_storage"],
        dataset_version=dataset_version
    )
    
    # Compile results
    results = {
        "collected_count": len(documents),
        "processed_count": len(processed_documents),
        "storage_results": storage_results,
        "completed_at": datetime.utcnow().isoformat()
    }
    
    flow_logger.info(f"Pipeline completed: {results}")
    return results
