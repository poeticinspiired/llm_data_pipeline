"""
Test script for data storage module.

This script demonstrates the usage of the data storage module
with sample data from the data processing module.
"""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_collection.main import create_collector, collect_sample
from src.data_processing.main import create_default_pipeline, process_sample
from src.data_storage.main import create_mongodb_storage, create_local_storage, store_processed_documents


def test_mongodb_storage():
    """Test MongoDB storage with sample processed documents."""
    print("Testing MongoDB storage...")
    
    # Create sample data
    sample_dir = Path("data/samples")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    sample_file = sample_dir / "storage_test_sample.jsonl"
    
    # Create a sample file if it doesn't exist
    if not sample_file.exists():
        with open(sample_file, 'w') as f:
            f.write('{"id": "doc1", "text": "This is a sample legal document text from the Supreme Court.", "court": "Supreme Court", "year": 2020}\n')
            f.write('{"id": "doc2", "text": "Another sample document with different content.", "court": "Circuit Court", "year": 2021}\n')
    
    # Create a collector for the sample file
    collector = create_collector(
        source_type="generic_jsonl",
        name="storage_test_sample",
        local_path=sample_file,
        metadata={"text_field": "text", "id_field": "id"}
    )
    
    # Collect sample documents
    print("Collecting sample documents...")
    documents = collect_sample(collector, sample_size=2)
    
    # Process the documents
    print("Processing documents...")
    pipeline = create_default_pipeline()
    processed_docs = process_sample(documents, sample_size=2, pipeline=pipeline)
    
    # Create MongoDB storage
    # Using a connection string for a local MongoDB instance
    # In a real environment, this would be a connection to a MongoDB server
    print("Creating MongoDB storage...")
    mongodb_storage = create_mongodb_storage(
        connection_string="mongodb://localhost:27017/",
        database_name="llm_data_pipeline_test",
        collection_name="processed_documents"
    )
    
    # Check if MongoDB is available
    if not mongodb_storage.connect():
        print("MongoDB is not available. Skipping MongoDB storage test.")
        return False
    
    # Store the processed documents
    print("Storing processed documents in MongoDB...")
    results = store_processed_documents(mongodb_storage, processed_docs)
    
    # Print results
    success_count = sum(1 for success in results.values() if success)
    print(f"Successfully stored {success_count} out of {len(results)} documents in MongoDB")
    
    # Retrieve the documents
    print("Retrieving documents from MongoDB...")
    for doc_id in results.keys():
        retrieved_doc = mongodb_storage.get_document(doc_id)
        if retrieved_doc:
            print(f"  Retrieved document {doc_id}: {retrieved_doc.text[:50]}...")
        else:
            print(f"  Failed to retrieve document {doc_id}")
    
    return success_count > 0


def test_local_storage():
    """Test local storage with sample data."""
    print("Testing local storage...")
    
    # Create local storage
    storage_dir = Path("data/local_storage_test")
    storage_dir.mkdir(parents=True, exist_ok=True)
    
    local_storage = create_local_storage(storage_dir)
    
    # Store a text file
    print("Storing text in local storage...")
    text_content = "This is a sample text file for testing local storage."
    text_path = "test/sample_text.txt"
    text_metadata = {"source": "test", "type": "text", "created": "2025-05-25"}
    
    text_result = local_storage.store_text(text_content, text_path, text_metadata)
    print(f"  Text storage result: {text_result}")
    
    # Store a file
    print("Storing file in local storage...")
    sample_file = Path("data/samples/storage_test_sample.jsonl")
    file_path = "test/sample_file.jsonl"
    file_metadata = {"source": "test", "type": "jsonl", "created": "2025-05-25"}
    
    file_result = local_storage.store_file(sample_file, file_path, file_metadata)
    print(f"  File storage result: {file_result}")
    
    # List files
    print("Listing files in local storage...")
    files = local_storage.list_files("test/")
    print(f"  Found {len(files)} files: {files}")
    
    # Retrieve text
    print("Retrieving text from local storage...")
    retrieved_text = local_storage.get_text(text_path)
    if retrieved_text:
        print(f"  Retrieved text: {retrieved_text[:50]}...")
    else:
        print("  Failed to retrieve text")
    
    # Retrieve file
    print("Retrieving file from local storage...")
    retrieved_file_path = Path("data/retrieved_sample_file.jsonl")
    file_retrieval_result = local_storage.get_file(file_path, retrieved_file_path)
    print(f"  File retrieval result: {file_retrieval_result}")
    
    if file_retrieval_result and retrieved_file_path.exists():
        with open(retrieved_file_path, 'r') as f:
            content = f.read()
        print(f"  Retrieved file content: {content[:50]}...")
    
    # Clean up
    if retrieved_file_path.exists():
        os.remove(retrieved_file_path)
    
    return text_result and file_result


def main():
    """Run all tests."""
    success = True
    
    # Test local storage
    try:
        local_storage_success = test_local_storage()
        if local_storage_success:
            print("\nLocal storage test: PASSED")
        else:
            print("\nLocal storage test: FAILED")
            success = False
    except Exception as e:
        print(f"\nLocal storage test: ERROR - {e}")
        success = False
    
    print("\n" + "-" * 50 + "\n")
    
    # Test MongoDB storage
    try:
        mongodb_success = test_mongodb_storage()
        if mongodb_success:
            print("\nMongoDB storage test: PASSED")
        else:
            print("\nMongoDB storage test: SKIPPED (MongoDB not available)")
    except Exception as e:
        print(f"\nMongoDB storage test: ERROR - {e}")
    
    # Print overall result
    print("\n" + "=" * 50)
    if success:
        print("All available tests PASSED")
    else:
        print("Some tests FAILED")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
