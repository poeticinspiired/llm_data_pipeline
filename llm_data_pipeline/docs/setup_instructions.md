# Setup Instructions

This document provides detailed instructions for setting up and running the LLM Training Data Curation Pipeline.

## Prerequisites

- Python 3.8+
- MongoDB (local or remote instance)
- AWS account (optional, for S3 storage)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/llm-data-pipeline.git
cd llm-data-pipeline
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Download required NLTK data:
```bash
python -c "import nltk; nltk.download('punkt')"
```

5. Set up MongoDB:
   - Install MongoDB locally or use a cloud instance
   - Create a database for the pipeline (default: `llm_data_pipeline`)

6. Configure AWS credentials (optional, for S3 storage):
   - Set up AWS credentials in `~/.aws/credentials` or as environment variables

## Dependency Notes

The pipeline has specific version requirements for several packages to ensure compatibility:

- Prefect 2.7.7 is used for orchestration
- pydantic 1.10.8 is required for compatibility with Prefect
- httpx 0.23.0 is required for compatibility with Prefect's API client
- NLTK 3.9.1 with punkt tokenizer data is used for text processing

The code includes fallback mechanisms for NLTK tokenization in case the required data is not available, but it's recommended to download the data for optimal performance.

## Running the Pipeline

### Basic Usage

The pipeline can be run using the orchestration module:

```python
from src.orchestration.main import run_pipeline

# Run with default configuration
results = run_pipeline()

# Run with custom configuration
results = run_pipeline(
    source_type="court_listener",
    source_name="supreme_court",
    source_config={
        "bulk_dir": "/path/to/court_listener_data"
    },
    limit=1000,
    dataset_version="v1.0.0"
)
```

### Using Specific Data Sources

#### CourtListener Data

```python
from src.orchestration.main import run_court_listener_pipeline

results = run_court_listener_pipeline(
    bulk_dir="/path/to/court_listener_data",
    limit=1000,
    dataset_version="court_listener_v1"
)
```

#### Pile of Law Data

```python
from src.orchestration.main import run_pile_of_law_pipeline

results = run_pile_of_law_pipeline(
    jsonl_path="/path/to/pile_of_law.jsonl",
    limit=1000,
    dataset_version="pile_of_law_v1"
)
```

#### Generic JSONL Data

```python
from src.orchestration.main import run_generic_jsonl_pipeline

results = run_generic_jsonl_pipeline(
    jsonl_path="/path/to/documents.jsonl",
    text_field="text",
    id_field="id",
    limit=1000,
    dataset_version="custom_v1"
)
```

## Running Tests

To run the tests for each module:

```bash
# Test data collection
python -m tests.test_data_collection

# Test data processing
python -m tests.test_data_processing

# Test data storage
python -m tests.test_data_storage

# Run validation script
python -m tests.validate_pipeline
```

## Data Analysis

The repository includes Jupyter notebooks for data analysis:

```bash
# Start Jupyter notebook server
jupyter notebook notebooks/
```

Open the `data_analysis.ipynb` notebook to explore the processed data and quality metrics.

## Troubleshooting

### MongoDB Connection Issues

If you encounter MongoDB connection issues:

1. Ensure MongoDB is running:
   ```bash
   sudo systemctl status mongodb
   ```

2. Check MongoDB connection string in your configuration.

3. Verify that the MongoDB port (default: 27017) is accessible.

### NLTK Data Issues

If you encounter NLTK data-related errors:

1. Manually download the required NLTK data:
   ```python
   import nltk
   nltk.download('punkt')
   ```

2. The code includes fallback mechanisms for tokenization, but downloading the data is recommended for optimal performance.

### Prefect Orchestration Issues

If you encounter Prefect-related errors:

1. Ensure you're using the correct Prefect version (2.7.7) as specified in requirements.txt.

2. Check Prefect's documentation for any environment-specific setup requirements.

3. For local development, Prefect should work without additional configuration.

## Support

For questions or issues, please open an issue on the GitHub repository or contact the maintainers directly.
