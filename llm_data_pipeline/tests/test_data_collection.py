"""
Test script for data collection module.

This script demonstrates the usage of the data collection module
with sample data files.
"""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_collection.main import create_collector, collect_sample


def test_court_listener_collector():
    """Test the CourtListener collector with a sample CSV file."""
    # Create a sample CSV file
    sample_dir = Path("data/samples")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    sample_file = sample_dir / "court_listener_sample.csv"
    
    # Create a sample CSV file if it doesn't exist
    if not sample_file.exists():
        with open(sample_file, 'w') as f:
            f.write("id,case_name,court_id,date_filed,plain_text\n")
            f.write("1,Smith v. Jones,ca1,2020-01-01,This is a sample court opinion text.\n")
            f.write("2,Doe v. Government,ca2,2020-02-01,Another sample court opinion with more text content.\n")
            f.write("3,Company v. Regulator,ca3,2020-03-01,A third sample with different content for testing.\n")
    
    # Create a collector for the sample file
    collector = create_collector(
        source_type="court_listener",
        name="court_listener_sample",
        local_path=sample_file,
        metadata={"data_type": "opinions"}
    )
    
    # Collect a sample of documents
    print("Testing CourtListener collector...")
    documents = collect_sample(collector, sample_size=3)
    
    # Print the results
    print(f"Collected {len(documents)} documents")
    for i, doc in enumerate(documents):
        print(f"Document {i+1}:")
        print(f"  ID: {doc.id}")
        print(f"  Text: {doc.text[:50]}...")
        print(f"  Metadata: {doc.metadata}")
        print()
    
    return len(documents) > 0


def test_pile_of_law_collector():
    """Test the Pile of Law collector with a sample JSONL file."""
    # Create a sample JSONL file
    sample_dir = Path("data/samples")
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    sample_file = sample_dir / "pile_of_law_sample.jsonl"
    
    # Create a sample JSONL file if it doesn't exist
    if not sample_file.exists():
        with open(sample_file, 'w') as f:
            f.write('{"id": "doc1", "text": "This is a sample legal document text.", "court": "Supreme Court", "year": 2020}\n')
            f.write('{"id": "doc2", "text": "Another sample document with different content.", "court": "Circuit Court", "year": 2021}\n')
            f.write('{"id": "doc3", "text": "A third sample for testing the collector.", "court": "District Court", "year": 2022}\n')
    
    # Create a collector for the sample file
    collector = create_collector(
        source_type="pile_of_law",
        name="pile_of_law_sample",
        local_path=sample_file,
        metadata={"text_field": "text", "id_field": "id"}
    )
    
    # Collect a sample of documents
    print("Testing Pile of Law collector...")
    documents = collect_sample(collector, sample_size=3)
    
    # Print the results
    print(f"Collected {len(documents)} documents")
    for i, doc in enumerate(documents):
        print(f"Document {i+1}:")
        print(f"  ID: {doc.id}")
        print(f"  Text: {doc.text[:50]}...")
        print(f"  Metadata: {doc.metadata}")
        print()
    
    return len(documents) > 0


def main():
    """Run all tests."""
    success = True
    
    # Test CourtListener collector
    try:
        cl_success = test_court_listener_collector()
        if cl_success:
            print("CourtListener collector test: PASSED")
        else:
            print("CourtListener collector test: FAILED (no documents collected)")
            success = False
    except Exception as e:
        print(f"CourtListener collector test: ERROR - {e}")
        success = False
    
    print("\n" + "-" * 50 + "\n")
    
    # Test Pile of Law collector
    try:
        pol_success = test_pile_of_law_collector()
        if pol_success:
            print("Pile of Law collector test: PASSED")
        else:
            print("Pile of Law collector test: FAILED (no documents collected)")
            success = False
    except Exception as e:
        print(f"Pile of Law collector test: ERROR - {e}")
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
