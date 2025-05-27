"""
Base classes and interfaces for data processing components.

This module defines the abstract base classes and interfaces that all data
processing components must implement, ensuring a consistent API across
different processing strategies.
"""

import abc
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union

# Import Document class from data collection module
from ..data_collection.base import Document


class ProcessingStage(Enum):
    """Enumeration of processing stages in the pipeline."""
    CLEANING = "cleaning"
    TOKENIZATION = "tokenization"
    FILTERING = "filtering"
    DEDUPLICATION = "deduplication"
    QUALITY_ASSESSMENT = "quality_assessment"
    METADATA_ENRICHMENT = "metadata_enrichment"


@dataclass
class ProcessedDocument:
    """Represents a document that has been processed."""
    # Original document ID and source information
    id: str
    source: str
    source_id: str
    
    # Processed text content
    text: str
    
    # Tokenization results (if applicable)
    tokens: Optional[List[str]] = None
    token_count: Optional[int] = None
    
    # Quality metrics
    quality_score: Optional[float] = None
    quality_metrics: Dict[str, float] = field(default_factory=dict)
    
    # Processing metadata
    processing_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Original metadata (preserved from source)
    original_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Enhanced metadata (added during processing)
    enhanced_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Processing history
    processing_history: List[str] = field(default_factory=list)
    
    @classmethod
    def from_document(cls, doc: Document) -> 'ProcessedDocument':
        """Create a ProcessedDocument from a raw Document.
        
        Args:
            doc: Raw document from data collection.
            
        Returns:
            ProcessedDocument: Initial processed document.
        """
        return cls(
            id=doc.id,
            source=doc.source,
            source_id=doc.source_id,
            text=doc.text,
            original_metadata=doc.metadata.copy(),
            processing_history=["initial_import"]
        )
    
    def add_processing_step(self, step_name: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Record a processing step in the document's history.
        
        Args:
            step_name: Name of the processing step.
            metadata: Optional metadata about the processing step.
        """
        self.processing_history.append(step_name)
        if metadata:
            self.processing_metadata[step_name] = metadata


class Processor(abc.ABC):
    """Abstract base class for all document processors."""
    
    @abc.abstractmethod
    def process(self, document: Union[Document, ProcessedDocument]) -> ProcessedDocument:
        """Process a single document.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document.
        """
        pass
    
    @abc.abstractmethod
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        pass
    
    @abc.abstractmethod
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        pass


class BatchProcessor(abc.ABC):
    """Abstract base class for batch document processors."""
    
    @abc.abstractmethod
    def process_batch(
        self, 
        documents: List[Union[Document, ProcessedDocument]]
    ) -> List[ProcessedDocument]:
        """Process a batch of documents.
        
        Args:
            documents: Batch of documents to process.
            
        Returns:
            List[ProcessedDocument]: Processed documents.
        """
        pass
    
    @abc.abstractmethod
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        pass
    
    @abc.abstractmethod
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        pass


class ProcessingPipeline:
    """A pipeline of document processors."""
    
    def __init__(self, processors: List[Union[Processor, BatchProcessor]]):
        """Initialize the processing pipeline.
        
        Args:
            processors: List of processors to apply in sequence.
        """
        self.processors = processors
        
        # Validate that we have at least one processor
        if not processors:
            raise ValueError("Processing pipeline must have at least one processor")
            
        # Group processors by stage
        self.processors_by_stage = {}
        for processor in processors:
            stage = processor.get_stage()
            if stage not in self.processors_by_stage:
                self.processors_by_stage[stage] = []
            self.processors_by_stage[stage].append(processor)
    
    def process_document(self, document: Union[Document, ProcessedDocument]) -> ProcessedDocument:
        """Process a single document through the entire pipeline.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document.
        """
        # Convert to ProcessedDocument if needed
        if isinstance(document, Document):
            doc = ProcessedDocument.from_document(document)
        else:
            doc = document
            
        # Apply each processor in sequence
        for processor in self.processors:
            if isinstance(processor, Processor):
                doc = processor.process(doc)
                
        return doc
    
    def process_batch(
        self, 
        documents: List[Union[Document, ProcessedDocument]],
        batch_size: int = 100
    ) -> Generator[ProcessedDocument, None, None]:
        """Process a batch of documents through the entire pipeline.
        
        This method handles both individual and batch processors efficiently.
        
        Args:
            documents: Batch of documents to process.
            batch_size: Size of sub-batches for batch processors.
            
        Yields:
            ProcessedDocument: Processed documents, one at a time.
        """
        # Convert all documents to ProcessedDocument if needed
        processed_docs = []
        for doc in documents:
            if isinstance(doc, Document):
                processed_docs.append(ProcessedDocument.from_document(doc))
            else:
                processed_docs.append(doc)
                
        # Process documents through each stage
        current_batch = processed_docs
        
        for processor in self.processors:
            # Handle batch processors
            if isinstance(processor, BatchProcessor):
                # Process in sub-batches to manage memory
                all_results = []
                for i in range(0, len(current_batch), batch_size):
                    sub_batch = current_batch[i:i+batch_size]
                    results = processor.process_batch(sub_batch)
                    all_results.extend(results)
                current_batch = all_results
            # Handle individual processors
            else:
                current_batch = [processor.process(doc) for doc in current_batch]
                
        # Yield processed documents
        for doc in current_batch:
            yield doc
            
    def get_stages(self) -> List[ProcessingStage]:
        """Get the stages in this pipeline.
        
        Returns:
            List[ProcessingStage]: The stages in order.
        """
        stages = []
        for processor in self.processors:
            stage = processor.get_stage()
            if stage not in stages:
                stages.append(stage)
        return stages
