"""
Validation script for the LLM Training Data Curation Pipeline.

This script performs end-to-end validation of the pipeline, testing all components
from data collection to storage and analysis.
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data_collection.main import create_collector, collect_sample
from src.data_processing.main import create_default_pipeline, process_sample
from src.data_storage.main import create_mongodb_storage, create_local_storage
from src.orchestration.main import run_generic_jsonl_pipeline


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_sample_data():
    """Create sample data for validation."""
    logger.info("Creating sample data for validation")
    
    # Create sample directory
    sample_dir = project_root / "data" / "samples"
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    # Create a sample JSONL file
    sample_file = sample_dir / "validation_sample.jsonl"
    
    # Create sample data if it doesn't exist
    if not sample_file.exists():
        logger.info(f"Creating sample file: {sample_file}")
        with open(sample_file, 'w') as f:
            # Add 10 sample documents
            for i in range(10):
                doc = {
                    "id": f"sample-{i+1}",
                    "text": f"This is a sample legal document {i+1} for validation testing. "
                           f"The case of Smith v. Jones, 123 U.S. 456 (2020), established an important precedent. "
                           f"The Court held that under 42 U.S.C. § 1983, plaintiffs must show...",
                    "court": "Supreme Court",
                    "year": 2020 + (i % 5)
                }
                f.write(json.dumps(doc) + "\n")
    
    return sample_file


def validate_data_collection():
    """Validate the data collection module."""
    logger.info("Validating data collection module")
    
    # Create sample data
    sample_file = create_sample_data()
    
    # Create a collector for the sample file
    collector = create_collector(
        source_type="generic_jsonl",
        name="validation_sample",
        local_path=sample_file,
        metadata={"text_field": "text", "id_field": "id"}
    )
    
    # Collect sample documents
    documents = collect_sample(collector, sample_size=5)
    
    # Validate results
    assert len(documents) > 0, "No documents collected"
    assert all(doc.id for doc in documents), "Documents missing IDs"
    assert all(doc.text for doc in documents), "Documents missing text"
    
    logger.info(f"Successfully collected {len(documents)} documents")
    return documents


def validate_data_processing(documents):
    """Validate the data processing module."""
    logger.info("Validating data processing module")
    
    # Create default processing pipeline
    pipeline = create_default_pipeline()
    
    # Process the sample documents
    processed_docs = process_sample(documents, sample_size=5, pipeline=pipeline)
    
    # Validate results
    assert len(processed_docs) > 0, "No documents processed"
    assert all(doc.quality_score is not None for doc in processed_docs), "Documents missing quality scores"
    assert all(doc.token_count > 0 for doc in processed_docs), "Documents have no tokens"
    
    logger.info(f"Successfully processed {len(processed_docs)} documents")
    return processed_docs


def validate_data_storage(processed_docs):
    """Validate the data storage module."""
    logger.info("Validating data storage module")
    
    # Create local storage for testing
    storage_dir = project_root / "data" / "validation_storage"
    storage_dir.mkdir(parents=True, exist_ok=True)
    
    local_storage = create_local_storage(storage_dir)
    
    # Store a sample document
    if processed_docs:
        doc = processed_docs[0]
        remote_path = f"test/{doc.id}.txt"
        metadata = {
            "source": doc.source,
            "quality_score": doc.quality_score,
            "token_count": doc.token_count
        }
        
        # Store the document
        result = local_storage.store_text(doc.text, remote_path, metadata)
        assert result, "Failed to store document"
        
        # Retrieve the document
        retrieved_text = local_storage.get_text(remote_path)
        assert retrieved_text == doc.text, "Retrieved text doesn't match original"
        
        # List files
        files = local_storage.list_files("test/")
        assert len(files) > 0, "No files listed"
        
        logger.info(f"Successfully validated storage with {len(files)} files")
        return True
    
    return False


def validate_orchestration():
    """Validate the orchestration module."""
    logger.info("Validating orchestration module")
    
    # Create sample data
    sample_file = create_sample_data()
    
    try:
        # Run a small pipeline with the sample data
        results = run_generic_jsonl_pipeline(
            jsonl_path=str(sample_file),
            text_field="text",
            id_field="id",
            limit=3,
            dataset_version=f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        # Check results
        assert results, "No results returned from pipeline"
        assert "collected_count" in results, "Missing collected_count in results"
        assert "processed_count" in results, "Missing processed_count in results"
        assert "storage_results" in results, "Missing storage_results in results"
        
        logger.info(f"Successfully validated orchestration: {results}")
        return True
    except Exception as e:
        logger.error(f"Orchestration validation failed: {e}")
        return False


def run_validation():
    """Run all validation tests."""
    logger.info("Starting pipeline validation")
    
    validation_results = {
        "data_collection": False,
        "data_processing": False,
        "data_storage": False,
        "orchestration": False
    }
    
    try:
        # Validate data collection
        documents = validate_data_collection()
        validation_results["data_collection"] = bool(documents)
        
        # Validate data processing
        if validation_results["data_collection"]:
            processed_docs = validate_data_processing(documents)
            validation_results["data_processing"] = bool(processed_docs)
            
            # Validate data storage
            if validation_results["data_processing"]:
                validation_results["data_storage"] = validate_data_storage(processed_docs)
        
        # Validate orchestration
        validation_results["orchestration"] = validate_orchestration()
        
        # Overall validation result
        validation_success = all(validation_results.values())
        
        if validation_success:
            logger.info("All validation tests passed!")
        else:
            failed_components = [comp for comp, result in validation_results.items() if not result]
            logger.warning(f"Validation failed for components: {', '.join(failed_components)}")
        
        return validation_success
    
    except Exception as e:
        logger.exception(f"Validation failed with error: {e}")
        return False


if __name__ == "__main__":
    success = run_validation()
    sys.exit(0 if success else 1)
