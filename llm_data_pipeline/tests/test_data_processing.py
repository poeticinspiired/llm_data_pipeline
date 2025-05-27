"""
Test script for data processing module.

This script demonstrates the usage of the data processing module
with sample data from the data collection module.
"""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_collection.main import create_collector, collect_sample
from src.data_processing.main import create_default_pipeline, process_sample


def test_processing_pipeline():
    """Test the data processing pipeline with sample data."""
    print("Testing data processing pipeline...")
    
    # Create sample data directory if it doesn't exist
    sample_dir = Path("data/samples")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    # Create a sample JSONL file for testing
    sample_file = sample_dir / "processing_test_sample.jsonl"
    
    # Create a sample file if it doesn't exist
    if not sample_file.exists():
        with open(sample_file, 'w') as f:
            f.write('{"id": "doc1", "text": "This is a sample legal document text from the Supreme Court. The case of Smith v. Jones, 123 U.S. 456 (2020), established an important precedent. The Court held that under 42 U.S.C. § 1983, plaintiffs must show...", "court": "Supreme Court", "year": 2020}\n')
            f.write('{"id": "doc2", "text": "CONFIDENTIAL DOCUMENT\\n\\nCase No. ABC-123\\n\\nPage 1 of 5\\n\\nIn the matter of Johnson v. Department, the plaintiff argues that pursuant to Section 230, immunity should be granted. The Court disagrees and finds that...", "court": "Circuit Court", "year": 2021}\n')
            f.write('{"id": "doc3", "text": "1  Lorem ipsum dolor sit amet.\\n2  Consectetur adipiscing elit.\\n3  Sed do eiusmod tempor incididunt.\\n\\n- Page 2 -\\n\\n4  Ut labore et dolore magna aliqua.\\n5  Ut enim ad minim veniam.", "court": "District Court", "year": 2022}\n')
    
    # Create a collector for the sample file
    collector = create_collector(
        source_type="generic_jsonl",
        name="processing_test_sample",
        local_path=sample_file,
        metadata={"text_field": "text", "id_field": "id"}
    )
    
    # Collect sample documents
    print("Collecting sample documents...")
    documents = collect_sample(collector, sample_size=3)
    
    # Create default processing pipeline
    pipeline = create_default_pipeline()
    
    # Process the sample documents
    print("Processing documents...")
    processed_docs = process_sample(documents, sample_size=3, pipeline=pipeline)
    
    # Print the results
    print(f"Processed {len(processed_docs)} documents")
    for i, doc in enumerate(processed_docs):
        print(f"\nDocument {i+1}:")
        print(f"  ID: {doc.id}")
        print(f"  Original length: {len(doc.text)}")
        print(f"  Quality score: {doc.quality_score:.2f}")
        print(f"  Token count: {doc.token_count}")
        print(f"  Processing steps: {doc.processing_history}")
        print(f"  Filtered: {doc.processing_metadata.get('filtered', False)}")
        if "sentences" in doc.processing_metadata:
            print(f"  Sentence count: {len(doc.processing_metadata['sentences'])}")
            print(f"  First sentence: {doc.processing_metadata['sentences'][0]}")
    
    return len(processed_docs) > 0


def main():
    """Run all tests."""
    success = True
    
    # Test processing pipeline
    try:
        pipeline_success = test_processing_pipeline()
        if pipeline_success:
            print("\nProcessing pipeline test: PASSED")
        else:
            print("\nProcessing pipeline test: FAILED (no documents processed)")
            success = False
    except Exception as e:
        print(f"\nProcessing pipeline test: ERROR - {e}")
        success = False
    
    # Print overall result
    print("\n" + "=" * 50)
    if success:
        print("All tests PASSED")
    else:
        print("Some tests FAILED")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
