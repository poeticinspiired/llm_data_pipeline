"""
MongoDB storage implementation for processed documents.

This module provides components for storing processed documents in MongoDB,
with support for indexing, querying, and versioning.
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

from ..data_processing.base import ProcessedDocument


logger = logging.getLogger(__name__)


class MongoDBStorage:
    """MongoDB storage for processed documents."""
    
    def __init__(
        self,
        connection_string: str,
        database_name: str,
        collection_name: str,
        create_indexes: bool = True
    ):
        """Initialize MongoDB storage.
        
        Args:
            connection_string: MongoDB connection string.
            database_name: Name of the database to use.
            collection_name: Name of the collection to use.
            create_indexes: Whether to create indexes on the collection.
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.create_indexes = create_indexes
        
        self.client = None
        self.db = None
        self.collection = None
        
    def connect(self) -> bool:
        """Connect to MongoDB.
        
        Returns:
            bool: True if connection was successful, False otherwise.
        """
        try:
            # Connect to MongoDB
            self.client = MongoClient(self.connection_string)
            
            # Check connection
            self.client.admin.command('ping')
            
            # Get database and collection
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            # Create indexes if requested
            if self.create_indexes:
                self._create_indexes()
                
            logger.info(f"Connected to MongoDB: {self.database_name}.{self.collection_name}")
            return True
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
            
        except Exception as e:
            logger.exception(f"Error connecting to MongoDB: {e}")
            return False
            
    def _create_indexes(self) -> None:
        """Create indexes on the collection."""
        try:
            # Create indexes for common query fields
            self.collection.create_index("id", unique=True)
            self.collection.create_index("source")
            self.collection.create_index("source_id")
            self.collection.create_index("quality_score")
            self.collection.create_index("token_count")
            self.collection.create_index("metadata.filtered")
            self.collection.create_index("metadata.duplicate")
            self.collection.create_index("metadata.dataset_version")
            
            # Create text index for full-text search
            self.collection.create_index([("text", pymongo.TEXT)])
            
            logger.info(f"Created indexes on {self.collection_name}")
            
        except OperationFailure as e:
            logger.error(f"Failed to create indexes: {e}")
            
    def store_document(self, document: ProcessedDocument) -> bool:
        """Store a processed document in MongoDB.
        
        Args:
            document: Processed document to store.
            
        Returns:
            bool: True if storage was successful, False otherwise.
        """
        if not self.collection:
            if not self.connect():
                return False
                
        try:
            # Convert ProcessedDocument to dict
            doc_dict = self._document_to_dict(document)
            
            # Add storage metadata
            doc_dict["storage_metadata"] = {
                "stored_at": datetime.utcnow(),
                "version": "1.0"
            }
            
            # Insert or update document
            result = self.collection.update_one(
                {"id": document.id},
                {"$set": doc_dict},
                upsert=True
            )
            
            return result.acknowledged
            
        except Exception as e:
            logger.exception(f"Error storing document {document.id}: {e}")
            return False
            
    def store_documents(self, documents: List[ProcessedDocument]) -> Dict[str, bool]:
        """Store multiple processed documents in MongoDB.
        
        Args:
            documents: List of processed documents to store.
            
        Returns:
            Dict[str, bool]: Dictionary mapping document IDs to storage success.
        """
        if not self.collection:
            if not self.connect():
                return {doc.id: False for doc in documents}
                
        results = {}
        
        try:
            # Prepare bulk operations
            operations = []
            
            for document in documents:
                # Convert ProcessedDocument to dict
                doc_dict = self._document_to_dict(document)
                
                # Add storage metadata
                doc_dict["storage_metadata"] = {
                    "stored_at": datetime.utcnow(),
                    "version": "1.0"
                }
                
                # Add update operation
                operations.append(
                    pymongo.UpdateOne(
                        {"id": document.id},
                        {"$set": doc_dict},
                        upsert=True
                    )
                )
                
            # Execute bulk operations
            if operations:
                result = self.collection.bulk_write(operations, ordered=False)
                
                # Track results
                for document in documents:
                    results[document.id] = True
                    
            return results
            
        except Exception as e:
            logger.exception(f"Error in bulk document storage: {e}")
            
            # Mark all as failed
            for document in documents:
                results[document.id] = False
                
            return results
            
    def get_document(self, document_id: str) -> Optional[ProcessedDocument]:
        """Retrieve a document from MongoDB by ID.
        
        Args:
            document_id: ID of the document to retrieve.
            
        Returns:
            Optional[ProcessedDocument]: Retrieved document, or None if not found.
        """
        if not self.collection:
            if not self.connect():
                return None
                
        try:
            # Query for document
            doc_dict = self.collection.find_one({"id": document_id})
            
            if not doc_dict:
                return None
                
            # Convert dict to ProcessedDocument
            return self._dict_to_document(doc_dict)
            
        except Exception as e:
            logger.exception(f"Error retrieving document {document_id}: {e}")
            return None
            
    def query_documents(
        self,
        query: Dict[str, Any],
        limit: Optional[int] = None,
        skip: Optional[int] = None,
        sort_by: Optional[List[tuple]] = None
    ) -> List[ProcessedDocument]:
        """Query documents from MongoDB.
        
        Args:
            query: MongoDB query dictionary.
            limit: Optional limit on number of results.
            skip: Optional number of documents to skip.
            sort_by: Optional list of (field, direction) tuples for sorting.
            
        Returns:
            List[ProcessedDocument]: List of retrieved documents.
        """
        if not self.collection:
            if not self.connect():
                return []
                
        try:
            # Prepare cursor
            cursor = self.collection.find(query)
            
            # Apply skip if provided
            if skip is not None:
                cursor = cursor.skip(skip)
                
            # Apply limit if provided
            if limit is not None:
                cursor = cursor.limit(limit)
                
            # Apply sorting if provided
            if sort_by:
                cursor = cursor.sort(sort_by)
                
            # Convert results to ProcessedDocuments
            return [self._dict_to_document(doc) for doc in cursor]
            
        except Exception as e:
            logger.exception(f"Error querying documents: {e}")
            return []
            
    def count_documents(self, query: Dict[str, Any]) -> int:
        """Count documents matching a query.
        
        Args:
            query: MongoDB query dictionary.
            
        Returns:
            int: Number of matching documents.
        """
        if not self.collection:
            if not self.connect():
                return 0
                
        try:
            return self.collection.count_documents(query)
            
        except Exception as e:
            logger.exception(f"Error counting documents: {e}")
            return 0
            
    def delete_document(self, document_id: str) -> bool:
        """Delete a document from MongoDB by ID.
        
        Args:
            document_id: ID of the document to delete.
            
        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        if not self.collection:
            if not self.connect():
                return False
                
        try:
            result = self.collection.delete_one({"id": document_id})
            return result.deleted_count == 1
            
        except Exception as e:
            logger.exception(f"Error deleting document {document_id}: {e}")
            return False
            
    def create_dataset_version(
        self,
        version_name: str,
        query: Optional[Dict[str, Any]] = None
    ) -> int:
        """Create a new dataset version by tagging matching documents.
        
        Args:
            version_name: Name of the dataset version.
            query: Optional query to filter documents for this version.
            
        Returns:
            int: Number of documents in the version.
        """
        if not self.collection:
            if not self.connect():
                return 0
                
        try:
            # Use empty query if none provided
            if query is None:
                query = {}
                
            # Add version tag to matching documents
            result = self.collection.update_many(
                query,
                {
                    "$set": {
                        "metadata.dataset_version": version_name,
                        "metadata.version_created_at": datetime.utcnow()
                    }
                }
            )
            
            return result.modified_count
            
        except Exception as e:
            logger.exception(f"Error creating dataset version {version_name}: {e}")
            return 0
            
    def _document_to_dict(self, document: ProcessedDocument) -> Dict[str, Any]:
        """Convert a ProcessedDocument to a dictionary for MongoDB storage.
        
        Args:
            document: ProcessedDocument to convert.
            
        Returns:
            Dict[str, Any]: Dictionary representation of the document.
        """
        # Create base dictionary
        doc_dict = {
            "id": document.id,
            "source": document.source,
            "source_id": document.source_id,
            "text": document.text,
            "tokens": document.tokens,
            "token_count": document.token_count,
            "quality_score": document.quality_score,
            "quality_metrics": document.quality_metrics,
            "original_metadata": document.original_metadata,
            "enhanced_metadata": document.enhanced_metadata,
            "processing_history": document.processing_history,
            "metadata": document.processing_metadata
        }
        
        return doc_dict
        
    def _dict_to_document(self, doc_dict: Dict[str, Any]) -> ProcessedDocument:
        """Convert a dictionary from MongoDB to a ProcessedDocument.
        
        Args:
            doc_dict: Dictionary from MongoDB.
            
        Returns:
            ProcessedDocument: Converted document.
        """
        # Create ProcessedDocument
        doc = ProcessedDocument(
            id=doc_dict["id"],
            source=doc_dict["source"],
            source_id=doc_dict["source_id"],
            text=doc_dict["text"]
        )
        
        # Set additional fields
        doc.tokens = doc_dict.get("tokens")
        doc.token_count = doc_dict.get("token_count")
        doc.quality_score = doc_dict.get("quality_score")
        doc.quality_metrics = doc_dict.get("quality_metrics", {})
        doc.original_metadata = doc_dict.get("original_metadata", {})
        doc.enhanced_metadata = doc_dict.get("enhanced_metadata", {})
        doc.processing_history = doc_dict.get("processing_history", [])
        doc.processing_metadata = doc_dict.get("metadata", {})
        
        return doc
        
    def close(self) -> None:
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            self.collection = None
            logger.info("Closed MongoDB connection")
