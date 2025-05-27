"""
Text cleaning processors for legal text data.

This module provides processors for cleaning and normalizing legal text,
including handling of legal-specific formatting, citations, and artifacts.
"""

import re
import logging
import unicodedata
from typing import Any, Dict, List, Optional, Pattern, Set, Tuple, Union

from .base import Processor, ProcessedDocument, ProcessingStage


logger = logging.getLogger(__name__)


class BasicTextCleaner(Processor):
    """Basic text cleaner for common text normalization tasks."""
    
    def __init__(
        self,
        normalize_whitespace: bool = True,
        normalize_unicode: bool = True,
        remove_urls: bool = True,
        remove_emails: bool = True,
        lowercase: bool = False,
        max_consecutive_newlines: int = 3,
        max_line_length: Optional[int] = None
    ):
        """Initialize the basic text cleaner.
        
        Args:
            normalize_whitespace: Whether to normalize whitespace.
            normalize_unicode: Whether to normalize Unicode characters.
            remove_urls: Whether to remove URLs.
            remove_emails: Whether to remove email addresses.
            lowercase: Whether to convert text to lowercase.
            max_consecutive_newlines: Maximum number of consecutive newlines to allow.
            max_line_length: Maximum line length (longer lines will be split).
        """
        self.normalize_whitespace = normalize_whitespace
        self.normalize_unicode = normalize_unicode
        self.remove_urls = remove_urls
        self.remove_emails = remove_emails
        self.lowercase = lowercase
        self.max_consecutive_newlines = max_consecutive_newlines
        self.max_line_length = max_line_length
        
        # Compile regular expressions for efficiency
        self.url_pattern = re.compile(
            r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^/\s]*)*'
        )
        self.email_pattern = re.compile(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        )
        self.whitespace_pattern = re.compile(r'\s+')
        self.newline_pattern = re.compile(r'\n{' + str(max_consecutive_newlines + 1) + r',}')
        
    def process(self, document: Union[ProcessedDocument, Any]) -> ProcessedDocument:
        """Process a document by cleaning its text.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document with cleaned text.
        """
        # Ensure we have a ProcessedDocument
        if not isinstance(document, ProcessedDocument):
            raise TypeError("Expected ProcessedDocument")
            
        # Get the original text
        text = document.text
        original_length = len(text)
        
        # Apply cleaning steps
        if self.normalize_unicode:
            text = unicodedata.normalize('NFKC', text)
            
        if self.remove_urls:
            text = self.url_pattern.sub(' ', text)
            
        if self.remove_emails:
            text = self.email_pattern.sub(' ', text)
            
        if self.normalize_whitespace:
            # Replace tabs and other whitespace with a single space
            text = self.whitespace_pattern.sub(' ', text)
            
            # Limit consecutive newlines
            text = self.newline_pattern.sub('\n' * self.max_consecutive_newlines, text)
            
        if self.lowercase:
            text = text.lower()
            
        if self.max_line_length:
            # Split long lines
            lines = text.split('\n')
            new_lines = []
            for line in lines:
                if len(line) > self.max_line_length:
                    # Split at word boundaries if possible
                    words = line.split()
                    current_line = ""
                    for word in words:
                        if len(current_line) + len(word) + 1 <= self.max_line_length:
                            if current_line:
                                current_line += " " + word
                            else:
                                current_line = word
                        else:
                            new_lines.append(current_line)
                            current_line = word
                    if current_line:
                        new_lines.append(current_line)
                else:
                    new_lines.append(line)
            text = '\n'.join(new_lines)
            
        # Update the document
        document.text = text
        
        # Add processing metadata
        document.add_processing_step(
            "basic_text_cleaning",
            {
                "original_length": original_length,
                "cleaned_length": len(text),
                "chars_removed": original_length - len(text),
                "normalize_whitespace": self.normalize_whitespace,
                "normalize_unicode": self.normalize_unicode,
                "remove_urls": self.remove_urls,
                "remove_emails": self.remove_emails,
                "lowercase": self.lowercase
            }
        )
        
        return document
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.CLEANING
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "basic_text_cleaner"


class LegalTextCleaner(Processor):
    """Specialized cleaner for legal text with domain-specific cleaning rules."""
    
    def __init__(
        self,
        remove_header_footer: bool = True,
        remove_page_numbers: bool = True,
        normalize_citations: bool = True,
        remove_line_numbers: bool = True,
        remove_redactions: bool = True,
        normalize_section_markers: bool = True
    ):
        """Initialize the legal text cleaner.
        
        Args:
            remove_header_footer: Whether to remove common header/footer patterns.
            remove_page_numbers: Whether to remove page numbers.
            normalize_citations: Whether to normalize legal citations.
            remove_line_numbers: Whether to remove line numbers at the beginning of lines.
            remove_redactions: Whether to normalize redacted text.
            normalize_section_markers: Whether to normalize section markers.
        """
        self.remove_header_footer = remove_header_footer
        self.remove_page_numbers = remove_page_numbers
        self.normalize_citations = normalize_citations
        self.remove_line_numbers = remove_line_numbers
        self.remove_redactions = remove_redactions
        self.normalize_section_markers = normalize_section_markers
        
        # Compile regular expressions for efficiency
        self.page_number_pattern = re.compile(r'\n\s*-\s*\d+\s*-\s*\n')
        self.line_number_pattern = re.compile(r'^\s*\d{1,3}\s+', re.MULTILINE)
        self.redaction_pattern = re.compile(r'\[(?:REDACTED|redacted|Redacted|\*{2,})\]')
        self.section_pattern = re.compile(r'§+\s*(\d+)')
        
        # Common header/footer patterns in legal documents
        self.header_footer_patterns = [
            re.compile(r'(?i)CONFIDENTIAL'),
            re.compile(r'(?i)FILED UNDER SEAL'),
            re.compile(r'(?i)DOCUMENT SUBJECT TO PROTECTIVE ORDER'),
            re.compile(r'(?i)OFFICIAL TRANSCRIPT'),
            re.compile(r'(?i)CERTIFIED COPY'),
            re.compile(r'(?i)^\s*Page \d+ of \d+\s*$', re.MULTILINE),
            re.compile(r'(?i)^\s*Case No\.\s+[\w-]+\s*$', re.MULTILINE)
        ]
        
    def process(self, document: Union[ProcessedDocument, Any]) -> ProcessedDocument:
        """Process a document by cleaning its legal text.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document with cleaned legal text.
        """
        # Ensure we have a ProcessedDocument
        if not isinstance(document, ProcessedDocument):
            raise TypeError("Expected ProcessedDocument")
            
        # Get the original text
        text = document.text
        original_length = len(text)
        
        # Apply legal-specific cleaning steps
        if self.remove_page_numbers:
            text = self.page_number_pattern.sub('\n', text)
            
        if self.remove_line_numbers:
            text = self.line_number_pattern.sub('', text)
            
        if self.remove_redactions:
            text = self.redaction_pattern.sub('[REDACTED]', text)
            
        if self.normalize_section_markers:
            text = self.section_pattern.sub(r'Section \1', text)
            
        if self.remove_header_footer:
            for pattern in self.header_footer_patterns:
                text = pattern.sub('', text)
                
        if self.normalize_citations:
            # This is a simplified approach; real citation normalization would be more complex
            # Normalize "v." in case names
            text = re.sub(r'(?<=\w)\s+v\.\s+(?=\w)', ' v. ', text)
            
            # Normalize common citation formats
            text = re.sub(r'(\d+)\s*U\.S\.\s*(\d+)', r'\1 U.S. \2', text)
            text = re.sub(r'(\d+)\s*S\.\s*Ct\.\s*(\d+)', r'\1 S. Ct. \2', text)
            text = re.sub(r'(\d+)\s*F\.\s*(\d+)\s*(\d+)', r'\1 F.\2d \3', text)
            
        # Update the document
        document.text = text
        
        # Add processing metadata
        document.add_processing_step(
            "legal_text_cleaning",
            {
                "original_length": original_length,
                "cleaned_length": len(text),
                "chars_removed": original_length - len(text),
                "remove_header_footer": self.remove_header_footer,
                "remove_page_numbers": self.remove_page_numbers,
                "normalize_citations": self.normalize_citations,
                "remove_line_numbers": self.remove_line_numbers
            }
        )
        
        return document
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.CLEANING
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "legal_text_cleaner"


class TextNormalizer(Processor):
    """Normalizes text by applying various standardization rules."""
    
    def __init__(
        self,
        normalize_quotes: bool = True,
        normalize_dashes: bool = True,
        normalize_ellipses: bool = True,
        normalize_ampersands: bool = True,
        normalize_abbreviations: bool = False,
        expand_contractions: bool = False
    ):
        """Initialize the text normalizer.
        
        Args:
            normalize_quotes: Whether to normalize different quote characters.
            normalize_dashes: Whether to normalize different dash characters.
            normalize_ellipses: Whether to normalize ellipses.
            normalize_ampersands: Whether to normalize ampersands.
            normalize_abbreviations: Whether to normalize common abbreviations.
            expand_contractions: Whether to expand contractions (e.g., "don't" -> "do not").
        """
        self.normalize_quotes = normalize_quotes
        self.normalize_dashes = normalize_dashes
        self.normalize_ellipses = normalize_ellipses
        self.normalize_ampersands = normalize_ampersands
        self.normalize_abbreviations = normalize_abbreviations
        self.expand_contractions = expand_contractions
        
        # Define normalization mappings
        self.quote_chars = {
            '"': '"',  # left double quotation mark
            '"': '"',  # right double quotation mark
            '„': '"',  # double low-9 quotation mark
            '‟': '"',  # double high-reversed-9 quotation mark
            ''': "'",  # left single quotation mark
            ''': "'",  # right single quotation mark
            '‚': "'",  # single low-9 quotation mark
            '‛': "'",  # single high-reversed-9 quotation mark
        }
        
        self.dash_chars = {
            '–': '-',  # en dash
            '—': '-',  # em dash
            '―': '-',  # horizontal bar
            '‐': '-',  # hyphen
            '‑': '-',  # non-breaking hyphen
            '‒': '-',  # figure dash
            '−': '-',  # minus sign
        }
        
        # Common legal abbreviations
        self.abbreviations = {
            'U.S.': 'United States',
            'U.S.C.': 'United States Code',
            'C.F.R.': 'Code of Federal Regulations',
            'Fed. Reg.': 'Federal Register',
            'et al.': 'et alia',
            'et seq.': 'et sequentes',
            'i.e.': 'that is',
            'e.g.': 'for example',
        }
        
        # Common contractions
        self.contractions = {
            "ain't": "is not",
            "aren't": "are not",
            "can't": "cannot",
            "couldn't": "could not",
            "didn't": "did not",
            "doesn't": "does not",
            "don't": "do not",
            "hadn't": "had not",
            "hasn't": "has not",
            "haven't": "have not",
            "he'd": "he would",
            "he'll": "he will",
            "he's": "he is",
            "I'd": "I would",
            "I'll": "I will",
            "I'm": "I am",
            "I've": "I have",
            "isn't": "is not",
            "it's": "it is",
            "let's": "let us",
            "mightn't": "might not",
            "mustn't": "must not",
            "shan't": "shall not",
            "she'd": "she would",
            "she'll": "she will",
            "she's": "she is",
            "shouldn't": "should not",
            "that's": "that is",
            "there's": "there is",
            "they'd": "they would",
            "they'll": "they will",
            "they're": "they are",
            "they've": "they have",
            "we'd": "we would",
            "we're": "we are",
            "we've": "we have",
            "weren't": "were not",
            "what'll": "what will",
            "what're": "what are",
            "what's": "what is",
            "what've": "what have",
            "where's": "where is",
            "who'd": "who would",
            "who'll": "who will",
            "who're": "who are",
            "who's": "who is",
            "who've": "who have",
            "won't": "will not",
            "wouldn't": "would not",
            "you'd": "you would",
            "you'll": "you will",
            "you're": "you are",
            "you've": "you have"
        }
        
    def process(self, document: Union[ProcessedDocument, Any]) -> ProcessedDocument:
        """Process a document by normalizing its text.
        
        Args:
            document: Document to process.
            
        Returns:
            ProcessedDocument: Processed document with normalized text.
        """
        # Ensure we have a ProcessedDocument
        if not isinstance(document, ProcessedDocument):
            raise TypeError("Expected ProcessedDocument")
            
        # Get the original text
        text = document.text
        
        # Apply normalization steps
        if self.normalize_quotes:
            for char, replacement in self.quote_chars.items():
                text = text.replace(char, replacement)
                
        if self.normalize_dashes:
            for char, replacement in self.dash_chars.items():
                text = text.replace(char, replacement)
                
        if self.normalize_ellipses:
            text = re.sub(r'\.{2,}', '...', text)
            
        if self.normalize_ampersands:
            text = text.replace('&', 'and')
            
        if self.normalize_abbreviations:
            for abbr, expansion in self.abbreviations.items():
                # Use word boundaries to avoid partial matches
                text = re.sub(r'\b' + re.escape(abbr) + r'\b', expansion, text)
                
        if self.expand_contractions:
            for contraction, expansion in self.contractions.items():
                # Use word boundaries to avoid partial matches
                text = re.sub(r'\b' + re.escape(contraction) + r'\b', expansion, text)
                
        # Update the document
        document.text = text
        
        # Add processing metadata
        document.add_processing_step(
            "text_normalization",
            {
                "normalize_quotes": self.normalize_quotes,
                "normalize_dashes": self.normalize_dashes,
                "normalize_ellipses": self.normalize_ellipses,
                "normalize_ampersands": self.normalize_ampersands,
                "normalize_abbreviations": self.normalize_abbreviations,
                "expand_contractions": self.expand_contractions
            }
        )
        
        return document
    
    def get_stage(self) -> ProcessingStage:
        """Get the processing stage this processor belongs to.
        
        Returns:
            ProcessingStage: The processing stage.
        """
        return ProcessingStage.CLEANING
    
    def get_name(self) -> str:
        """Get the name of this processor.
        
        Returns:
            str: The processor name.
        """
        return "text_normalizer"
