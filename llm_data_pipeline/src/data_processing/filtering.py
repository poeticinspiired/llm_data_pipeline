"""
Filtering and quality assessment processors for text data.

This module provides processors for filtering and assessing the quality of text data,
with a focus on legal text and preparing data for language model training.
"""

import re
import logging
import hashlib
from typing import Any, Dict, List, Optional, Pattern, Set, Tuple, Union
from collections import Counter

from .base import Processor, BatchProcessor, ProcessedDocument, ProcessingStage


logger = logging.getLogger(__name__)


class QualityScorer(Processor):
    """Assesses the quality of text data based on various metrics."""
    
    def __init__(
        self,
        min_length: int = 100,
        max_length: Optional[int] = None,
        min_avg_word_length: float = 3.0,
        max_avg_word_length: float = 15.0,
        min_sentence_count: int = 3,
        max_repetition_ratio: float = 0.3,
        min_alphanumeric_ratio: float = 0.7,
        weights: Optional[Dict[str, float]] = None
    ):
        """Initialize the quality scorer.
        
        Args:
            min_length: Minimum text length in characters.
            max_length: Maximum text length in characters (None for no limit).
            min_avg_word_length: Minimum average word length.
            max_avg_word_length: Maximum average word length.
            min_sentence_count: Minimum number of sentences.
            max_repetition_ratio: Maximum ratio of repeated content.
            min_alphanumeric_ratio: Minimum ratio of alphanumeric characters.
            weights: Optional weights for different quality metrics.
        """
        self.min_length = min_length
        self.max_length = max_length
        self.min_avg_word_length = min_avg_word_length
        self.max_avg_word_length = max_avg_word_length
        self.min_sentence_count = min_sentence_count
        self.max_repetition_ratio = max_repetition_ratio
        self.min_alphanumeric_ratio = min_alphanumeric_ratio
        
        # Default weights if not provided
        self.weights = weights or {
            "length_score": 0.2,
            "avg_word_length_score": 0.1,
            "sentence_count_score": 0.2,
            "repetition_score": 0.2,
            "alphanumeric_score": 0.3
        }
        
    def process(self, document: Union[ProcessedDocument, Any]) -> ProcessedDocument:
        """Process a document by assessing its quality.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document with quality assessment.
        """
        # Ensure we have a ProcessedDocument
        if not isinstance(document, ProcessedDocument):
            raise TypeError("Expected ProcessedDocument")
            
        # Get the text
        text = document.text
        
        # Calculate quality metrics
        metrics = {}
        
        # Text length
        text_length = len(text)
        metrics["text_length"] = text_length
        
        if self.max_length is not None:
            length_score = min(1.0, max(0.0, 
                (text_length - self.min_length) / (self.max_length - self.min_length)
                if self.max_length > self.min_length else 0.0))
            # Penalize if too long
            if text_length > self.max_length:
                length_score = max(0.0, 1.0 - (text_length - self.max_length) / self.max_length)
        else:
            length_score = 1.0 if text_length >= self.min_length else text_length / self.min_length
            
        metrics["length_score"] = length_score
        
        # Word-based metrics
        words = text.split()
        word_count = len(words)
        metrics["word_count"] = word_count
        
        if word_count > 0:
            avg_word_length = sum(len(word) for word in words) / word_count
            metrics["avg_word_length"] = avg_word_length
            
            # Score based on average word length
            if avg_word_length < self.min_avg_word_length:
                avg_word_length_score = avg_word_length / self.min_avg_word_length
            elif avg_word_length > self.max_avg_word_length:
                avg_word_length_score = max(0.0, 1.0 - (avg_word_length - self.max_avg_word_length) / self.max_avg_word_length)
            else:
                avg_word_length_score = 1.0
        else:
            avg_word_length = 0
            avg_word_length_score = 0.0
            metrics["avg_word_length"] = avg_word_length
            
        metrics["avg_word_length_score"] = avg_word_length_score
        
        # Sentence count
        sentence_count = document.processing_metadata.get("sentence_count", 0)
        if sentence_count == 0:
            # Rough estimation if not already tokenized
            sentence_count = text.count('.') + text.count('!') + text.count('?')
            sentence_count = max(1, sentence_count)
            
        metrics["sentence_count"] = sentence_count
        sentence_count_score = min(1.0, sentence_count / self.min_sentence_count)
        metrics["sentence_count_score"] = sentence_count_score
        
        # Repetition detection
        if word_count > 0:
            # Count word frequencies
            word_freq = Counter(words)
            unique_words = len(word_freq)
            repetition_ratio = 1.0 - (unique_words / word_count)
            
            # Score based on repetition ratio
            repetition_score = 1.0 - (repetition_ratio / self.max_repetition_ratio)
            repetition_score = max(0.0, min(1.0, repetition_score))
        else:
            repetition_ratio = 1.0
            repetition_score = 0.0
            
        metrics["repetition_ratio"] = repetition_ratio
        metrics["repetition_score"] = repetition_score
        
        # Alphanumeric ratio
        alphanumeric_chars = sum(1 for c in text if c.isalnum())
        if text_length > 0:
            alphanumeric_ratio = alphanumeric_chars / text_length
        else:
            alphanumeric_ratio = 0.0
            
        metrics["alphanumeric_ratio"] = alphanumeric_ratio
        
        if alphanumeric_ratio < self.min_alphanumeric_ratio:
            alphanumeric_score = alphanumeric_ratio / self.min_alphanumeric_ratio
        else:
            alphanumeric_score = 1.0
            
        metrics["alphanumeric_score"] = alphanumeric_score
        
        # Calculate overall quality score
        quality_score = sum(
            metrics[metric] * weight
            for metric, weight in self.weights.items()
            if metric in metrics
        )
        
        # Store quality metrics and score
        document.quality_metrics = metrics
        document.quality_score = quality_score
        
        # Add processing step
        document.add_processing_step(
            "quality_assessment",
            {
                "quality_score": quality_score,
                **metrics
            }
        )
        
        return document
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.QUALITY_ASSESSMENT
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "quality_scorer"


class ContentFilter(Processor):
    """Filters documents based on content criteria."""
    
    def __init__(
        self,
        min_quality_score: float = 0.5,
        min_length: int = 100,
        max_length: Optional[int] = None,
        required_patterns: Optional[List[str]] = None,
        excluded_patterns: Optional[List[str]] = None,
        keep_document: bool = True
    ):
        """Initialize the content filter.
        
        Args:
            min_quality_score: Minimum quality score to pass the filter.
            min_length: Minimum text length in characters.
            max_length: Maximum text length in characters (None for no limit).
            required_patterns: List of regex patterns that must be present.
            excluded_patterns: List of regex patterns that must not be present.
            keep_document: If True, keep the document but mark it as filtered;
                          if False, return None for filtered documents.
        """
        self.min_quality_score = min_quality_score
        self.min_length = min_length
        self.max_length = max_length
        self.required_patterns = required_patterns or []
        self.excluded_patterns = excluded_patterns or []
        self.keep_document = keep_document
        
        # Compile patterns
        self.required_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in self.required_patterns]
        self.excluded_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in self.excluded_patterns]
        
    def process(self, document: Union[ProcessedDocument, Any]) -> Optional[ProcessedDocument]:
        """Process a document by filtering based on content criteria.
        
        Args:
            document: Document to process.
            
        Returns:
            Optional[ProcessedDocument]: Filtered document or None if filtered out.
        """
        # Ensure we have a ProcessedDocument
        if not isinstance(document, ProcessedDocument):
            raise TypeError("Expected ProcessedDocument")
            
        # Get the text
        text = document.text
        
        # Check length criteria
        text_length = len(text)
        if text_length < self.min_length:
            if not self.keep_document:
                return None
            document.processing_metadata["filtered"] = True
            document.processing_metadata["filter_reason"] = f"Text too short: {text_length} < {self.min_length}"
            return document
            
        if self.max_length is not None and text_length > self.max_length:
            if not self.keep_document:
                return None
            document.processing_metadata["filtered"] = True
            document.processing_metadata["filter_reason"] = f"Text too long: {text_length} > {self.max_length}"
            return document
            
        # Check quality score if available
        if document.quality_score is not None and document.quality_score < self.min_quality_score:
            if not self.keep_document:
                return None
            document.processing_metadata["filtered"] = True
            document.processing_metadata["filter_reason"] = f"Quality score too low: {document.quality_score} < {self.min_quality_score}"
            return document
            
        # Check required patterns
        for i, pattern in enumerate(self.required_regexes):
            if not pattern.search(text):
                if not self.keep_document:
                    return None
                document.processing_metadata["filtered"] = True
                document.processing_metadata["filter_reason"] = f"Missing required pattern: {self.required_patterns[i]}"
                return document
                
        # Check excluded patterns
        for i, pattern in enumerate(self.excluded_regexes):
            if pattern.search(text):
                if not self.keep_document:
                    return None
                document.processing_metadata["filtered"] = True
                document.processing_metadata["filter_reason"] = f"Contains excluded pattern: {self.excluded_patterns[i]}"
                return document
                
        # Document passed all filters
        document.processing_metadata["filtered"] = False
        
        # Add processing step
        document.add_processing_step(
            "content_filtering",
            {
                "passed": True,
                "min_quality_score": self.min_quality_score,
                "min_length": self.min_length,
                "max_length": self.max_length,
                "required_patterns_count": len(self.required_patterns),
                "excluded_patterns_count": len(self.excluded_patterns)
            }
        )
        
        return document
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.FILTERING
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "content_filter"


class Deduplicator(BatchProcessor):
    """Deduplicates documents based on content similarity."""
    
    def __init__(
        self,
        method: str = "exact",
        similarity_threshold: float = 0.9,
        hash_function: str = "md5",
        ngram_size: int = 3,
        keep_first: bool = True
    ):
        """Initialize the deduplicator.
        
        Args:
            method: Deduplication method ("exact", "simhash", or "minhash").
            similarity_threshold: Threshold for similarity-based deduplication.
            hash_function: Hash function for content hashing.
            ngram_size: Size of n-grams for similarity hashing.
            keep_first: If True, keep the first occurrence of duplicate documents.
        """
        self.method = method
        self.similarity_threshold = similarity_threshold
        self.hash_function = hash_function
        self.ngram_size = ngram_size
        self.keep_first = keep_first
        
        # Validate method
        if method not in ["exact", "simhash", "minhash"]:
            raise ValueError(f"Unsupported deduplication method: {method}")
            
        # Validate hash function
        if hash_function not in ["md5", "sha1", "sha256"]:
            raise ValueError(f"Unsupported hash function: {hash_function}")
            
    def process_batch(
        self, 
        documents: List[Union[ProcessedDocument, Any]]
    ) -> List[ProcessedDocument]:
        """Process a batch of documents by deduplicating them.
        
        Args:
            documents: Batch of documents to process.
            
        Returns:
            List[ProcessedDocument]: Deduplicated documents.
        """
        # Ensure we have ProcessedDocuments
        processed_docs = []
        for doc in documents:
            if not isinstance(doc, ProcessedDocument):
                raise TypeError("Expected ProcessedDocument")
            processed_docs.append(doc)
            
        # Apply deduplication based on method
        if self.method == "exact":
            return self._exact_deduplication(processed_docs)
        elif self.method == "simhash":
            return self._simhash_deduplication(processed_docs)
        elif self.method == "minhash":
            return self._minhash_deduplication(processed_docs)
        else:
            # Should never reach here due to validation in __init__
            return processed_docs
            
    def _exact_deduplication(self, documents: List[ProcessedDocument]) -> List[ProcessedDocument]:
        """Deduplicate documents based on exact content matching.
        
        Args:
            documents: Documents to deduplicate.
            
        Returns:
            List[ProcessedDocument]: Deduplicated documents.
        """
        # Hash function based on configuration
        if self.hash_function == "md5":
            hash_func = hashlib.md5
        elif self.hash_function == "sha1":
            hash_func = hashlib.sha1
        elif self.hash_function == "sha256":
            hash_func = hashlib.sha256
        else:
            hash_func = hashlib.md5
            
        # Calculate content hashes
        content_hashes = {}
        deduplicated = []
        duplicates = []
        
        for doc in documents:
            # Calculate hash of text content
            content_hash = hash_func(doc.text.encode('utf-8')).hexdigest()
            
            # Check if we've seen this hash before
            if content_hash in content_hashes:
                # Mark as duplicate
                doc.processing_metadata["duplicate"] = True
                doc.processing_metadata["duplicate_of"] = content_hashes[content_hash]
                duplicates.append(doc)
            else:
                # New unique document
                content_hashes[content_hash] = doc.id
                doc.processing_metadata["duplicate"] = False
                deduplicated.append(doc)
                
        # Add processing step to all documents
        for doc in documents:
            doc.add_processing_step(
                "deduplication",
                {
                    "method": "exact",
                    "hash_function": self.hash_function,
                    "is_duplicate": doc.processing_metadata.get("duplicate", False)
                }
            )
            
        # Return deduplicated documents or all documents with duplicate flags
        if self.keep_first:
            return deduplicated
        else:
            return documents
            
    def _simhash_deduplication(self, documents: List[ProcessedDocument]) -> List[ProcessedDocument]:
        """Deduplicate documents based on SimHash similarity.
        
        This is a simplified implementation of SimHash for demonstration.
        For production use, consider using a dedicated SimHash library.
        
        Args:
            documents: Documents to deduplicate.
            
        Returns:
            List[ProcessedDocument]: Deduplicated documents.
        """
        # Generate n-grams for a text
        def get_ngrams(text, n):
            return [text[i:i+n] for i in range(len(text) - n + 1)]
            
        # Calculate a simple simhash (simplified version)
        def calculate_simhash(text, n):
            ngrams = get_ngrams(text, n)
            if not ngrams:
                return 0
                
            # Use a fixed hash size of 64 bits
            hash_size = 64
            hash_vector = [0] * hash_size
            
            for ngram in ngrams:
                # Hash the n-gram
                ngram_hash = hash(ngram) % (2**hash_size)
                
                # Update hash vector
                for i in range(hash_size):
                    bit = (ngram_hash >> i) & 1
                    hash_vector[i] += 1 if bit else -1
                    
            # Convert to binary
            simhash = 0
            for i in range(hash_size):
                if hash_vector[i] > 0:
                    simhash |= (1 << i)
                    
            return simhash
            
        # Calculate Hamming distance between two hashes
        def hamming_distance(hash1, hash2):
            xor = hash1 ^ hash2
            distance = 0
            while xor:
                distance += 1
                xor &= xor - 1
            return distance
            
        # Calculate similarity from Hamming distance
        def calculate_similarity(hash1, hash2):
            distance = hamming_distance(hash1, hash2)
            # Convert to similarity (0-1 range)
            return 1.0 - (distance / 64.0)  # 64 is the hash size
            
        # Calculate simhashes for all documents
        simhashes = {}
        deduplicated = []
        duplicates = []
        
        for doc in documents:
            # Calculate simhash
            doc_simhash = calculate_simhash(doc.text, self.ngram_size)
            
            # Check for similar documents
            is_duplicate = False
            for unique_id, unique_hash in simhashes.items():
                similarity = calculate_similarity(doc_simhash, unique_hash)
                if similarity >= self.similarity_threshold:
                    # Mark as duplicate
                    doc.processing_metadata["duplicate"] = True
                    doc.processing_metadata["duplicate_of"] = unique_id
                    doc.processing_metadata["similarity"] = similarity
                    duplicates.append(doc)
                    is_duplicate = True
                    break
                    
            if not is_duplicate:
                # New unique document
                simhashes[doc.id] = doc_simhash
                doc.processing_metadata["duplicate"] = False
                deduplicated.append(doc)
                
        # Add processing step to all documents
        for doc in documents:
            doc.add_processing_step(
                "deduplication",
                {
                    "method": "simhash",
                    "ngram_size": self.ngram_size,
                    "similarity_threshold": self.similarity_threshold,
                    "is_duplicate": doc.processing_metadata.get("duplicate", False),
                    "similarity": doc.processing_metadata.get("similarity", 1.0)
                }
            )
            
        # Return deduplicated documents or all documents with duplicate flags
        if self.keep_first:
            return deduplicated
        else:
            return documents
            
    def _minhash_deduplication(self, documents: List[ProcessedDocument]) -> List[ProcessedDocument]:
        """Deduplicate documents based on MinHash similarity.
        
        This is a simplified implementation of MinHash for demonstration.
        For production use, consider using a dedicated MinHash library.
        
        Args:
            documents: Documents to deduplicate.
            
        Returns:
            List[ProcessedDocument]: Deduplicated documents.
        """
        # For simplicity, we'll use a very basic MinHash implementation
        # In production, you would use a library like datasketch
        
        # Generate shingles (n-grams) for a text
        def get_shingles(text, n):
            return set(text[i:i+n] for i in range(len(text) - n + 1))
            
        # Calculate Jaccard similarity between two sets
        def jaccard_similarity(set1, set2):
            if not set1 or not set2:
                return 0.0
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            return intersection / union
            
        # Extract shingles for all documents
        doc_shingles = {}
        deduplicated = []
        duplicates = []
        
        for doc in documents:
            # Extract shingles
            shingles = get_shingles(doc.text, self.ngram_size)
            
            # Check for similar documents
            is_duplicate = False
            for unique_id, unique_shingles in doc_shingles.items():
                similarity = jaccard_similarity(shingles, unique_shingles)
                if similarity >= self.similarity_threshold:
                    # Mark as duplicate
                    doc.processing_metadata["duplicate"] = True
                    doc.processing_metadata["duplicate_of"] = unique_id
                    doc.processing_metadata["similarity"] = similarity
                    duplicates.append(doc)
                    is_duplicate = True
                    break
                    
            if not is_duplicate:
                # New unique document
                doc_shingles[doc.id] = shingles
                doc.processing_metadata["duplicate"] = False
                deduplicated.append(doc)
                
        # Add processing step to all documents
        for doc in documents:
            doc.add_processing_step(
                "deduplication",
                {
                    "method": "minhash",
                    "ngram_size": self.ngram_size,
                    "similarity_threshold": self.similarity_threshold,
                    "is_duplicate": doc.processing_metadata.get("duplicate", False),
                    "similarity": doc.processing_metadata.get("similarity", 1.0)
                }
            )
            
        # Return deduplicated documents or all documents with duplicate flags
        if self.keep_first:
            return deduplicated
        else:
            return documents
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.DEDUPLICATION
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "deduplicator"
