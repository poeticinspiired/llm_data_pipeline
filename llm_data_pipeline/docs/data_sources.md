# Selected Data Sources for LLM Training Data Curation

This document outlines the selected data sources for our LLM training data curation pipeline, focusing on legal text. These sources have been chosen based on their public availability, data quality, comprehensive coverage, and compliance with legal and ethical requirements.

## Primary Data Sources

### 1. CourtListener Bulk Data

**Source Overview:**
- **Provider:** Free Law Project (nonprofit organization)
- **URL:** https://www.courtlistener.com/help/api/bulk-data/
- **Content Type:** Legal case law, court opinions, dockets, and related legal documents
- **License:** Free of known copyright restrictions (public domain)
- **Update Frequency:** Monthly (last day of each month)

**Data Format:**
- CSV files generated using PostgreSQL `COPY TO` command
- UTF-8 encoding with header rows
- Files correspond to database tables with well-defined schemas
- Files are complete snapshots, not deltas

**Available Datasets:**
- Courts metadata
- Dockets (case information)
- Opinion Clusters and Opinions (full text of court opinions)
- Citations Map (which opinions cite which)
- Parentheticals (short summaries of opinions)
- Judge data
- Oral argument data

**Access Method:**
- Files are stored in AWS S3 bucket
- Direct download links available through the browsable interface
- No authentication required
- Files are named with generation time (UTC) and object type

**Advantages:**
- Comprehensive coverage of U.S. legal system
- Well-structured data with clear schema
- Regular updates
- Public domain status
- Established provider with long-term stability
- Includes both raw text and metadata

**Technical Considerations:**
- Large file sizes (opinions bulk data file is the largest)
- PostgreSQL-compatible format simplifies database import
- Schema documentation available through code repository and API

### 2. Pile of Law Dataset

**Source Overview:**
- **Provider:** Pile of Law project
- **URL:** https://huggingface.co/datasets/pile-of-law/pile-of-law
- **Content Type:** Large corpus of legal and administrative data
- **License:** CC-BY-NC-SA-4.0
- **Size:** Between 10M and 100M documents

**Data Format:**
- JSONL files (compressed with XZ)
- Multiple file segments for larger subcollections
- Includes training and validation splits

**Available Subcollections:**
- Court opinions (from CourtListener and other sources)
- Docket entries
- Contracts
- Bills and statutes
- Administrative decisions
- Regulatory documents
- International legal documents
- Legal commentary

**Access Method:**
- Direct download from Hugging Face
- Programmatic access via Hugging Face Datasets API
- No authentication required for download

**Advantages:**
- Pre-processed for machine learning
- Diverse range of legal document types
- International coverage (not just U.S.)
- Includes both case law and regulatory/administrative texts
- Ready-to-use format for NLP tasks
- Academic research backing

**Technical Considerations:**
- Large compressed files (multiple GB)
- Requires decompression before processing
- Non-commercial license restrictions
- English-dominant but includes some other languages

## Selection Rationale

These two sources were selected based on the following criteria:

1. **Complementary Coverage:** CourtListener provides deep coverage of U.S. case law, while Pile of Law offers broader document type diversity and some international coverage.

2. **Data Quality:** Both sources maintain high-quality, well-structured data suitable for LLM training.

3. **Public Accessibility:** Both sources are freely available without authentication barriers.

4. **Legal Compliance:** Both sources contain data that is either public domain or available under clear licensing terms.

5. **Format Suitability:** The data formats (CSV and JSONL) are well-suited for processing in our pipeline architecture.

6. **Scale:** Both sources provide sufficient scale (millions of documents) to train a small-scale language model.

7. **Metadata Richness:** Both include valuable metadata beyond raw text, enabling better filtering and quality assessment.

## Compliance Considerations

- **CourtListener:** Data is explicitly marked as free of known copyright restrictions, making it suitable for any use case.
- **Pile of Law:** Licensed under CC-BY-NC-SA-4.0, which allows non-commercial use with attribution and share-alike requirements. This restricts commercial applications but is suitable for research and educational purposes.

## Implementation Strategy

Our pipeline will:

1. Begin with a subset of CourtListener opinion data as the primary source
2. Supplement with selected subcollections from Pile of Law for diversity
3. Implement source-specific adapters for each data format
4. Track data provenance throughout the pipeline
5. Respect all licensing requirements in the final dataset

## Alternative Sources (Not Selected)

Several other sources were considered but not selected as primary sources:

- **Harvard Caselaw Access Project:** Requires API key and has usage limitations
- **Federal Court Cases Integrated Database:** Less comprehensive than CourtListener
- **Individual court websites:** Would require multiple scrapers and less consistent data
- **LexisNexis/Westlaw:** Commercial services with strict licensing

These alternative sources may be considered for future pipeline extensions if needed.
