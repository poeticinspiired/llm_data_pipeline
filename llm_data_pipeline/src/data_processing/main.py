"""
Main entry point for the data processing module.

This module provides a simple interface for processing documents through
various cleaning, tokenization, filtering, and quality assessment steps.
"""

import logging
from typing import Dict, Generator, List, Optional, Union

from ..data_collection.base import Document
from .base import Processor, BatchProcessor, ProcessedDocument, ProcessingPipeline, ProcessingStage
from .cleaning import BasicTextCleaner, LegalTextCleaner, TextNormalizer
from .tokenization import SentenceTokenizer, WordTokenizer, LegalTokenizer
from .filtering import QualityScorer, ContentFilter, Deduplicator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_default_pipeline() -> ProcessingPipeline:
    """Create a default processing pipeline for legal text.
    
    Returns:
        ProcessingPipeline: A pipeline with standard processors.
    """
    processors = [
        # Cleaning stage
        BasicTextCleaner(
            normalize_whitespace=True,
            normalize_unicode=True,
            remove_urls=True,
            remove_emails=True,
            lowercase=False,
            max_consecutive_newlines=3
        ),
        LegalTextCleaner(
            remove_header_footer=True,
            remove_page_numbers=True,
            normalize_citations=True,
            remove_line_numbers=True
        ),
        TextNormalizer(
            normalize_quotes=True,
            normalize_dashes=True,
            normalize_ellipses=True,
            normalize_ampersands=False,
            normalize_abbreviations=False
        ),
        
        # Tokenization stage
        SentenceTokenizer(
            language='english',
            store_sentence_spans=False,
            min_sentence_length=3
        ),
        WordTokenizer(
            language='english',
            lowercase=False,
            remove_punctuation=False,
            min_word_length=1
        ),
        
        # Quality assessment stage
        QualityScorer(
            min_length=100,
            min_avg_word_length=3.0,
            max_avg_word_length=15.0,
            min_sentence_count=3,
            max_repetition_ratio=0.3,
            min_alphanumeric_ratio=0.7
        ),
        
        # Filtering stage
        ContentFilter(
            min_quality_score=0.5,
            min_length=100,
            keep_document=True
        )
    ]
    
    # Batch processors are added separately
    batch_processors = [
        # Deduplication stage
        Deduplicator(
            method="exact",
            hash_function="md5",
            keep_first=True
        )
    ]
    
    # Combine all processors
    all_processors = processors + batch_processors
    
    return ProcessingPipeline(all_processors)


def create_custom_pipeline(
    processors: List[Union[Processor, BatchProcessor]]
) -> ProcessingPipeline:
    """Create a custom processing pipeline with specified processors.
    
    Args:
        processors: List of processors to include in the pipeline.
        
    Returns:
        ProcessingPipeline: A pipeline with the specified processors.
    """
    return ProcessingPipeline(processors)


def process_documents(
    documents: List[Document],
    pipeline: Optional[ProcessingPipeline] = None,
    batch_size: int = 100
) -> Generator[ProcessedDocument, None, None]:
    """Process a list of documents through a processing pipeline.
    
    Args:
        documents: List of documents to process.
        pipeline: Processing pipeline to use (default pipeline if None).
        batch_size: Size of batches for batch processing.
        
    Yields:
        ProcessedDocument: Processed documents.
    """
    # Use default pipeline if none provided
    if pipeline is None:
        pipeline = create_default_pipeline()
        
    # Process documents in batches
    logger.info(f"Processing {len(documents)} documents with batch size {batch_size}")
    
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} documents)")
        
        for processed_doc in pipeline.process_batch(batch, batch_size=batch_size):
            yield processed_doc
            
    logger.info("Document processing complete")


def process_sample(
    documents: List[Document],
    sample_size: int = 10,
    pipeline: Optional[ProcessingPipeline] = None
) -> List[ProcessedDocument]:
    """Process a sample of documents for testing and validation.
    
    Args:
        documents: List of documents to sample from.
        sample_size: Number of documents to process.
        pipeline: Processing pipeline to use (default pipeline if None).
        
    Returns:
        List[ProcessedDocument]: Processed sample documents.
    """
    # Limit to sample size
    sample = documents[:min(sample_size, len(documents))]
    
    # Process the sample
    return list(process_documents(sample, pipeline, batch_size=sample_size))
