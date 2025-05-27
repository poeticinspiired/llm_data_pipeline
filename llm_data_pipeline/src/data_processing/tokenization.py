"""
Tokenization processors for text data.

This module provides processors for tokenizing text data, with a focus on
legal text and preparing data for language model training.
"""

import re
import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import nltk
from nltk.tokenize import sent_tokenize as nltk_sent_tokenize, word_tokenize as nltk_word_tokenize

def sent_tokenize(text, language='english'):
    """
    Wrapper for NLTK's sent_tokenize that handles the punkt_tab error.
    Falls back to a simple regex-based tokenizer if NLTK's tokenizer fails.
    """
    try:
        return nltk_sent_tokenize(text, language=language)
    except LookupError:
        # Simple fallback using regex
        import re
        return re.split(r'(?<=[.!?])\s+', text)

def word_tokenize(text, language='english', preserve_line=False):
    """
    Wrapper for NLTK's word_tokenize that handles the punkt_tab error.
    Falls back to a simple regex-based tokenizer if NLTK's tokenizer fails.
    """
    try:
        return nltk_word_tokenize(text, language=language)
    except LookupError:
        # Simple fallback using regex
        import re
        # First split into sentences if not preserving lines
        if not preserve_line:
            sentences = sent_tokenize(text, language=language)
        else:
            sentences = [text]
        
        # Then split each sentence into words
        words = []
        for sentence in sentences:
            # Split on whitespace and punctuation
            tokens = re.findall(r'\w+|[^\w\s]', sentence)
            words.extend(tokens)
        return words

from .base import Processor, ProcessedDocument, ProcessingStage


logger = logging.getLogger(__name__)


# Ensure NLTK resources are available
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    logger.info("Downloading NLTK punkt tokenizer")
    nltk.download('punkt', quiet=True)


class SentenceTokenizer(Processor):
    """Tokenizes text into sentences."""
    
    def __init__(
        self,
        language: str = 'english',
        store_sentence_spans: bool = False,
        min_sentence_length: int = 3,
        max_sentence_length: Optional[int] = None
    ):
        """Initialize the sentence tokenizer.
        
        Args:
            language: Language for tokenization rules.
            store_sentence_spans: Whether to store character spans for each sentence.
            min_sentence_length: Minimum sentence length in words.
            max_sentence_length: Maximum sentence length in words (None for no limit).
        """
        self.language = language
        self.store_sentence_spans = store_sentence_spans
        self.min_sentence_length = min_sentence_length
        self.max_sentence_length = max_sentence_length
        
    def process(self, document: Union[ProcessedDocument, Any]) -> ProcessedDocument:
        """Process a document by tokenizing its text into sentences.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document with sentence tokenization.
        """
        # Ensure we have a ProcessedDocument
        if not isinstance(document, ProcessedDocument):
            raise TypeError("Expected ProcessedDocument")
            
        # Get the text
        text = document.text
        
        # Tokenize into sentences
        sentences = sent_tokenize(text, language=self.language)
        
        # Filter sentences by length if needed
        if self.min_sentence_length > 0 or self.max_sentence_length:
            filtered_sentences = []
            for sentence in sentences:
                word_count = len(sentence.split())
                if word_count >= self.min_sentence_length:
                    if self.max_sentence_length is None or word_count <= self.max_sentence_length:
                        filtered_sentences.append(sentence)
            sentences = filtered_sentences
        
        # Store sentence spans if requested
        if self.store_sentence_spans:
            spans = []
            start = 0
            for sentence in sentences:
                # Find the sentence in the original text
                sentence_start = text.find(sentence, start)
                if sentence_start >= 0:
                    sentence_end = sentence_start + len(sentence)
                    spans.append((sentence_start, sentence_end))
                    start = sentence_end
            
            # Store spans in processing metadata
            document.processing_metadata["sentence_spans"] = spans
        
        # Store sentences in processing metadata
        document.processing_metadata["sentences"] = sentences
        document.processing_metadata["sentence_count"] = len(sentences)
        
        # Add processing step
        document.add_processing_step(
            "sentence_tokenization",
            {
                "language": self.language,
                "sentence_count": len(sentences),
                "min_sentence_length": self.min_sentence_length,
                "max_sentence_length": self.max_sentence_length
            }
        )
        
        return document
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.TOKENIZATION
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "sentence_tokenizer"


class WordTokenizer(Processor):
    """Tokenizes text into words."""
    
    def __init__(
        self,
        language: str = 'english',
        lowercase: bool = False,
        remove_punctuation: bool = False,
        min_word_length: int = 1,
        max_word_length: Optional[int] = None,
        store_token_spans: bool = False
    ):
        """Initialize the word tokenizer.
        
        Args:
            language: Language for tokenization rules.
            lowercase: Whether to lowercase all tokens.
            remove_punctuation: Whether to remove punctuation tokens.
            min_word_length: Minimum word length.
            max_word_length: Maximum word length (None for no limit).
            store_token_spans: Whether to store character spans for each token.
        """
        self.language = language
        self.lowercase = lowercase
        self.remove_punctuation = remove_punctuation
        self.min_word_length = min_word_length
        self.max_word_length = max_word_length
        self.store_token_spans = store_token_spans
        
        # Punctuation pattern for filtering
        self.punctuation_pattern = re.compile(r'^\W+$')
        
    def process(self, document: Union[ProcessedDocument, Any]) -> ProcessedDocument:
        """Process a document by tokenizing its text into words.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document with word tokenization.
        """
        # Ensure we have a ProcessedDocument
        if not isinstance(document, ProcessedDocument):
            raise TypeError("Expected ProcessedDocument")
            
        # Get the text
        text = document.text
        
        # Check if we have sentences from previous processing
        if "sentences" in document.processing_metadata:
            # Tokenize each sentence separately
            all_tokens = []
            for sentence in document.processing_metadata["sentences"]:
                tokens = word_tokenize(sentence, language=self.language)
                all_tokens.extend(tokens)
        else:
            # Tokenize the entire text
            all_tokens = word_tokenize(text, language=self.language)
        
        # Apply filters
        filtered_tokens = []
        for token in all_tokens:
            # Apply lowercase if requested
            if self.lowercase:
                token = token.lower()
                
            # Skip punctuation if requested
            if self.remove_punctuation and self.punctuation_pattern.match(token):
                continue
                
            # Check length constraints
            if len(token) < self.min_word_length:
                continue
                
            if self.max_word_length is not None and len(token) > self.max_word_length:
                continue
                
            filtered_tokens.append(token)
        
        # Store tokens in the document
        document.tokens = filtered_tokens
        document.token_count = len(filtered_tokens)
        
        # Store token spans if requested
        if self.store_token_spans:
            # This is a simplified approach that may not work perfectly for all cases
            spans = []
            start = 0
            for token in filtered_tokens:
                token_start = text.find(token, start)
                if token_start >= 0:
                    token_end = token_start + len(token)
                    spans.append((token_start, token_end))
                    start = token_end
            
            # Store spans in processing metadata
            document.processing_metadata["token_spans"] = spans
        
        # Add processing step
        document.add_processing_step(
            "word_tokenization",
            {
                "language": self.language,
                "token_count": len(filtered_tokens),
                "lowercase": self.lowercase,
                "remove_punctuation": self.remove_punctuation,
                "min_word_length": self.min_word_length,
                "max_word_length": self.max_word_length
            }
        )
        
        return document
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.TOKENIZATION
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "word_tokenizer"


class LegalTokenizer(Processor):
    """Specialized tokenizer for legal text with domain-specific rules."""
    
    def __init__(
        self,
        preserve_citations: bool = True,
        preserve_case_names: bool = True,
        preserve_statute_refs: bool = True,
        preserve_section_refs: bool = True,
        language: str = 'english'
    ):
        """Initialize the legal tokenizer.
        
        Args:
            preserve_citations: Whether to preserve legal citations as single tokens.
            preserve_case_names: Whether to preserve case names as single tokens.
            preserve_statute_refs: Whether to preserve statute references as single tokens.
            preserve_section_refs: Whether to preserve section references as single tokens.
            language: Base language for tokenization rules.
        """
        self.preserve_citations = preserve_citations
        self.preserve_case_names = preserve_case_names
        self.preserve_statute_refs = preserve_statute_refs
        self.preserve_section_refs = preserve_section_refs
        self.language = language
        
        # Patterns for legal-specific entities
        self.citation_pattern = re.compile(
            r'\d+\s+(?:U\.S\.|S\.\s*Ct\.|F\.\d+d)\s+\d+'
        )
        self.case_name_pattern = re.compile(
            r'[A-Z][a-zA-Z\'\-]+(?:\s+[A-Z][a-zA-Z\'\-]+)*\s+v\.\s+[A-Z][a-zA-Z\'\-]+(?:\s+[A-Z][a-zA-Z\'\-]+)*'
        )
        self.statute_pattern = re.compile(
            r'\d+\s+U\.S\.C\.\s+§+\s*\d+(?:[a-z])?'
        )
        self.section_pattern = re.compile(
            r'§+\s*\d+(?:\.\d+)*(?:[a-z])?'
        )
        
    def process(self, document: Union[ProcessedDocument, Any]) -> ProcessedDocument:
        """Process a document by tokenizing its legal text.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document with legal tokenization.
        """
        # Ensure we have a ProcessedDocument
        if not isinstance(document, ProcessedDocument):
            raise TypeError("Expected ProcessedDocument")
            
        # Get the text
        text = document.text
        
        # Extract legal-specific entities
        legal_entities = []
        entity_spans = []
        
        if self.preserve_citations:
            for match in self.citation_pattern.finditer(text):
                legal_entities.append(("citation", match.group(), match.span()))
                entity_spans.append(match.span())
                
        if self.preserve_case_names:
            for match in self.case_name_pattern.finditer(text):
                legal_entities.append(("case_name", match.group(), match.span()))
                entity_spans.append(match.span())
                
        if self.preserve_statute_refs:
            for match in self.statute_pattern.finditer(text):
                legal_entities.append(("statute", match.group(), match.span()))
                entity_spans.append(match.span())
                
        if self.preserve_section_refs:
            for match in self.section_pattern.finditer(text):
                legal_entities.append(("section", match.group(), match.span()))
                entity_spans.append(match.span())
        
        # Sort entities by span start position
        legal_entities.sort(key=lambda x: x[2][0])
        
        # Create a masked text for standard tokenization
        masked_text = text
        mask_map = {}
        offset = 0
        
        for entity_type, entity_text, (start, end) in legal_entities:
            # Adjust for previous replacements
            adj_start = start - offset
            adj_end = end - offset
            
            # Create a placeholder
            placeholder = f"__LEGAL_{entity_type.upper()}_{len(mask_map)}__"
            mask_map[placeholder] = entity_text
            
            # Replace in masked text
            masked_text = masked_text[:adj_start] + placeholder + masked_text[adj_end:]
            
            # Update offset
            offset += (end - start) - len(placeholder)
        
        # Tokenize the masked text
        tokens = word_tokenize(masked_text, language=self.language)
        
        # Restore legal entities
        final_tokens = []
        for token in tokens:
            if token in mask_map:
                # For legal entities, we might want to keep them as single tokens
                # or split them into component parts depending on the use case
                if self.preserve_citations or self.preserve_case_names or \
                   self.preserve_statute_refs or self.preserve_section_refs:
                    final_tokens.append(mask_map[token])
                else:
                    # Split the entity if not preserving
                    entity_tokens = word_tokenize(mask_map[token], language=self.language)
                    final_tokens.extend(entity_tokens)
            else:
                final_tokens.append(token)
        
        # Store tokens in the document
        document.tokens = final_tokens
        document.token_count = len(final_tokens)
        
        # Store legal entities in processing metadata
        document.processing_metadata["legal_entities"] = [
            {"type": entity_type, "text": entity_text, "span": span}
            for entity_type, entity_text, span in legal_entities
        ]
        
        # Add processing step
        document.add_processing_step(
            "legal_tokenization",
            {
                "token_count": len(final_tokens),
                "legal_entity_count": len(legal_entities),
                "preserve_citations": self.preserve_citations,
                "preserve_case_names": self.preserve_case_names,
                "preserve_statute_refs": self.preserve_statute_refs,
                "preserve_section_refs": self.preserve_section_refs
            }
        )
        
        return document
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.TOKENIZATION
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "legal_tokenizer"
