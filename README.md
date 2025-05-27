

# LLM Training Data Curation Pipeline

A scalable data pipeline for collecting, cleaning, and curating datasets for training small-scale language models, with a focus on legal text.

## Overview

This project implements a comprehensive data pipeline for curating high-quality training data for language models. It focuses on legal text (court rulings, opinions, etc.) and demonstrates best practices for building scalable, maintainable data processing systems.

The pipeline handles the entire data lifecycle:
1. **Collection** - Scraping and gathering legal text from various sources
2. **Processing** - Cleaning, tokenizing, and enhancing the raw text
3. **Quality Assessment** - Evaluating and filtering based on quality metrics
4. **Storage** - Storing processed data in MongoDB and raw data in cloud storage
5. **Orchestration** - Coordinating the pipeline with Prefect workflows

## Architecture

![image](https://github.com/user-attachments/assets/1428917d-cca1-4473-b2d9-93a312ffe4e6)

The pipeline follows a modular architecture with the following components:

- **Data Collection Module**: Interfaces with various data sources to collect legal text
- **Data Processing Module**: Cleans, tokenizes, and enhances the collected text
- **Data Storage Module**: Stores processed documents in MongoDB and raw data in cloud storage
- **Orchestration Module**: Coordinates the pipeline execution using Prefect

## Features

- **Modular Design**: Each component is independent and can be extended or replaced
- **Scalable Processing**: Handles large datasets through batch processing and streaming
- **Quality Assessment**: Evaluates document quality based on multiple metrics
- **Deduplication**: Identifies and removes duplicate or near-duplicate documents
- **Metadata Enhancement**: Extracts and enhances metadata from legal documents
- **Cloud Storage Integration**: Supports AWS S3 and local storage backends
- **MongoDB Integration**: Stores processed documents with indexing and querying capabilities
- **Workflow Orchestration**: Uses Prefect for reliable pipeline execution
- **Comprehensive Logging**: Tracks processing steps and document lineage
- **Data Analysis**: Includes Jupyter notebooks for dataset exploration and quality assessment

## Installation

### Prerequisites

- Python 3.8+
- MongoDB (local or remote instance)
- AWS account (optional, for S3 storage)

### Setup

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

4. Set up MongoDB:
   - Install MongoDB locally or use a cloud instance
   - Create a database for the pipeline (default: `llm_data_pipeline`)

5. Configure AWS credentials (optional, for S3 storage):
   - Set up AWS credentials in `~/.aws/credentials` or as environment variables

## Usage

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

### Running Tests

To run the tests for each module:

```bash
# Test data collection
python -m tests.test_data_collection

# Test data processing
python -m tests.test_data_processing

# Test data storage
python -m tests.test_data_storage
```

### Data Analysis

The repository includes Jupyter notebooks for data analysis:

```bash
# Start Jupyter notebook server
jupyter notebook notebooks/
```

Open the `data_analysis.ipynb` notebook to explore the processed data and quality metrics.

## Project Structure

```
llm_data_pipeline/
├── data/                  # Data directory for samples and local storage
├── docs/                  # Documentation
│   ├── architecture.md    # Detailed architecture documentation
│   ├── project_requirements.md  # Project requirements and scope
│   └── data_sources.md    # Information about supported data sources
├── notebooks/             # Jupyter notebooks for analysis
│   └── data_analysis.ipynb  # Sample data analysis notebook
├── src/                   # Source code
│   ├── data_collection/   # Data collection module
│   ├── data_processing/   # Data processing module
│   ├── data_storage/      # Data storage module
│   └── orchestration/     # Pipeline orchestration module
├── tests/                 # Test scripts
├── .gitignore             # Git ignore file
├── README.md              # This file
└── requirements.txt       # Python dependencies
```

## Module Details

### Data Collection Module

The data collection module is responsible for gathering legal text from various sources. It supports:

- **CourtListener**: Collects court opinions from the CourtListener bulk data
- **Pile of Law**: Processes data from the Pile of Law dataset
- **Generic JSONL**: Handles custom JSONL files with configurable field mapping

Each collector implements a common interface, making it easy to add new data sources.

### Data Processing Module

The data processing module transforms raw text into clean, structured documents. It includes:

- **Text Cleaning**: Removes headers, footers, page numbers, and other artifacts
- **Tokenization**: Splits text into sentences and words with specialized legal tokenization
- **Quality Assessment**: Evaluates document quality based on multiple metrics
- **Deduplication**: Identifies and removes duplicate or near-duplicate documents
- **Filtering**: Filters documents based on quality criteria and content rules

The processing pipeline is configurable and can be customized for different use cases.

### Data Storage Module

The data storage module handles persistent storage of processed documents and raw data:

- **MongoDB Storage**: Stores processed documents with indexing and querying capabilities
- **Cloud Storage**: Supports AWS S3 and local storage for raw data
- **Versioning**: Tracks dataset versions for reproducibility

### Orchestration Module

The orchestration module coordinates the pipeline execution using Prefect:

- **Workflow Definition**: Defines the pipeline as a Prefect flow
- **Task Management**: Breaks down the pipeline into manageable tasks
- **Error Handling**: Handles failures and retries
- **Monitoring**: Tracks pipeline execution and performance

## Data Quality Metrics

The pipeline evaluates document quality based on multiple metrics:

- **Text Length**: Document length in characters and tokens
- **Average Word Length**: Average length of words in the document
- **Sentence Count**: Number of sentences in the document
- **Repetition Ratio**: Measure of content repetition
- **Alphanumeric Ratio**: Ratio of alphanumeric characters to total characters

These metrics are combined into an overall quality score that can be used for filtering.

## Best Practices

- **Memory Efficiency**: Process large files in chunks to minimize memory usage
- **Error Handling**: Implement robust error handling and logging
- **Idempotency**: Ensure pipeline steps are idempotent for reliable reprocessing
- **Monitoring**: Track pipeline performance and document quality metrics
- **Versioning**: Version datasets for reproducibility and comparison
- **Testing**: Write tests for each module and component

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [CourtListener](https://www.courtlistener.com/) for providing access to legal opinions
- [Pile of Law](https://huggingface.co/datasets/pile-of-law/pile-of-law) dataset for legal text
- [Prefect](https://www.prefect.io/) for workflow orchestration
- [MongoDB](https://www.mongodb.com/) for document storage
- [NLTK](https://www.nltk.org/) for natural language processing
