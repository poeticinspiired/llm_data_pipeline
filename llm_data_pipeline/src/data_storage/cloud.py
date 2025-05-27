"""
Cloud storage implementation for raw data.

This module provides components for storing raw data in cloud storage solutions,
with support for AWS S3 and Azure Data Lake.
"""

import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, BinaryIO

import boto3
from botocore.exceptions import ClientError

from ..data_collection.base import Document


logger = logging.getLogger(__name__)


class CloudStorageBase:
    """Base class for cloud storage implementations."""
    
    def __init__(self):
        """Initialize cloud storage base."""
        pass
        
    def connect(self) -> bool:
        """Connect to cloud storage.
        
        Returns:
            bool: True if connection was successful, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement connect()")
        
    def store_file(
        self,
        local_path: Union[str, Path],
        remote_path: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """Store a local file in cloud storage.
        
        Args:
            local_path: Path to local file.
            remote_path: Path in cloud storage.
            metadata: Optional metadata to store with the file.
            
        Returns:
            bool: True if storage was successful, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement store_file()")
        
    def store_text(
        self,
        text: str,
        remote_path: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """Store text content in cloud storage.
        
        Args:
            text: Text content to store.
            remote_path: Path in cloud storage.
            metadata: Optional metadata to store with the file.
            
        Returns:
            bool: True if storage was successful, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement store_text()")
        
    def get_file(
        self,
        remote_path: str,
        local_path: Union[str, Path]
    ) -> bool:
        """Download a file from cloud storage.
        
        Args:
            remote_path: Path in cloud storage.
            local_path: Path to save the file locally.
            
        Returns:
            bool: True if download was successful, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement get_file()")
        
    def get_text(self, remote_path: str) -> Optional[str]:
        """Get text content from cloud storage.
        
        Args:
            remote_path: Path in cloud storage.
            
        Returns:
            Optional[str]: Text content, or None if not found.
        """
        raise NotImplementedError("Subclasses must implement get_text()")
        
    def list_files(self, prefix: str) -> List[str]:
        """List files in cloud storage with a given prefix.
        
        Args:
            prefix: Prefix to filter files.
            
        Returns:
            List[str]: List of file paths.
        """
        raise NotImplementedError("Subclasses must implement list_files()")
        
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from cloud storage.
        
        Args:
            remote_path: Path in cloud storage.
            
        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        raise NotImplementedError("Subclasses must implement delete_file()")
        
    def close(self) -> None:
        """Close the cloud storage connection."""
        pass


class S3Storage(CloudStorageBase):
    """AWS S3 storage implementation."""
    
    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None
    ):
        """Initialize S3 storage.
        
        Args:
            bucket_name: Name of the S3 bucket.
            aws_access_key_id: Optional AWS access key ID.
            aws_secret_access_key: Optional AWS secret access key.
            region_name: Optional AWS region name.
        """
        super().__init__()
        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        
        self.s3_client = None
        
    def connect(self) -> bool:
        """Connect to AWS S3.
        
        Returns:
            bool: True if connection was successful, False otherwise.
        """
        try:
            # Create S3 client
            kwargs = {}
            if self.aws_access_key_id and self.aws_secret_access_key:
                kwargs["aws_access_key_id"] = self.aws_access_key_id
                kwargs["aws_secret_access_key"] = self.aws_secret_access_key
            if self.region_name:
                kwargs["region_name"] = self.region_name
                
            self.s3_client = boto3.client("s3", **kwargs)
            
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            
            logger.info(f"Connected to S3 bucket: {self.bucket_name}")
            return True
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                logger.error(f"S3 bucket not found: {self.bucket_name}")
            elif error_code == "403":
                logger.error(f"Access denied to S3 bucket: {self.bucket_name}")
            else:
                logger.error(f"S3 connection error: {e}")
            return False
            
        except Exception as e:
            logger.exception(f"Error connecting to S3: {e}")
            return False
            
    def store_file(
        self,
        local_path: Union[str, Path],
        remote_path: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """Store a local file in S3.
        
        Args:
            local_path: Path to local file.
            remote_path: Path in S3 (key).
            metadata: Optional metadata to store with the file.
            
        Returns:
            bool: True if storage was successful, False otherwise.
        """
        if not self.s3_client:
            if not self.connect():
                return False
                
        try:
            # Ensure local_path is a string
            local_path_str = str(local_path)
            
            # Prepare upload parameters
            upload_args = {
                "Bucket": self.bucket_name,
                "Key": remote_path,
                "Body": open(local_path_str, "rb")
            }
            
            # Add metadata if provided
            if metadata:
                # S3 metadata values must be strings
                string_metadata = {k: str(v) for k, v in metadata.items()}
                upload_args["Metadata"] = string_metadata
                
            # Upload file
            self.s3_client.upload_file(
                local_path_str,
                self.bucket_name,
                remote_path,
                ExtraArgs=upload_args
            )
            
            logger.info(f"Stored file in S3: {remote_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Error storing file in S3: {e}")
            return False
            
    def store_text(
        self,
        text: str,
        remote_path: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """Store text content in S3.
        
        Args:
            text: Text content to store.
            remote_path: Path in S3 (key).
            metadata: Optional metadata to store with the file.
            
        Returns:
            bool: True if storage was successful, False otherwise.
        """
        if not self.s3_client:
            if not self.connect():
                return False
                
        try:
            # Prepare upload parameters
            upload_args = {
                "Bucket": self.bucket_name,
                "Key": remote_path,
                "Body": text.encode("utf-8")
            }
            
            # Add metadata if provided
            if metadata:
                # S3 metadata values must be strings
                string_metadata = {k: str(v) for k, v in metadata.items()}
                upload_args["Metadata"] = string_metadata
                
            # Upload text
            self.s3_client.put_object(**upload_args)
            
            logger.info(f"Stored text in S3: {remote_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Error storing text in S3: {e}")
            return False
            
    def get_file(
        self,
        remote_path: str,
        local_path: Union[str, Path]
    ) -> bool:
        """Download a file from S3.
        
        Args:
            remote_path: Path in S3 (key).
            local_path: Path to save the file locally.
            
        Returns:
            bool: True if download was successful, False otherwise.
        """
        if not self.s3_client:
            if not self.connect():
                return False
                
        try:
            # Ensure local_path is a string
            local_path_str = str(local_path)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path_str), exist_ok=True)
            
            # Download file
            self.s3_client.download_file(
                self.bucket_name,
                remote_path,
                local_path_str
            )
            
            logger.info(f"Downloaded file from S3: {remote_path} to {local_path_str}")
            return True
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                logger.error(f"File not found in S3: {remote_path}")
            else:
                logger.error(f"S3 download error: {e}")
            return False
            
        except Exception as e:
            logger.exception(f"Error downloading file from S3: {e}")
            return False
            
    def get_text(self, remote_path: str) -> Optional[str]:
        """Get text content from S3.
        
        Args:
            remote_path: Path in S3 (key).
            
        Returns:
            Optional[str]: Text content, or None if not found.
        """
        if not self.s3_client:
            if not self.connect():
                return None
                
        try:
            # Get object
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            
            # Read and decode content
            content = response["Body"].read().decode("utf-8")
            
            logger.info(f"Retrieved text from S3: {remote_path}")
            return content
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchKey":
                logger.error(f"File not found in S3: {remote_path}")
            else:
                logger.error(f"S3 retrieval error: {e}")
            return None
            
        except Exception as e:
            logger.exception(f"Error retrieving text from S3: {e}")
            return None
            
    def list_files(self, prefix: str) -> List[str]:
        """List files in S3 with a given prefix.
        
        Args:
            prefix: Prefix to filter files.
            
        Returns:
            List[str]: List of file keys.
        """
        if not self.s3_client:
            if not self.connect():
                return []
                
        try:
            # List objects
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            # Extract keys
            files = []
            if "Contents" in response:
                files = [obj["Key"] for obj in response["Contents"]]
                
            logger.info(f"Listed {len(files)} files in S3 with prefix: {prefix}")
            return files
            
        except Exception as e:
            logger.exception(f"Error listing files in S3: {e}")
            return []
            
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from S3.
        
        Args:
            remote_path: Path in S3 (key).
            
        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        if not self.s3_client:
            if not self.connect():
                return False
                
        try:
            # Delete object
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            
            logger.info(f"Deleted file from S3: {remote_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Error deleting file from S3: {e}")
            return False
            
    def close(self) -> None:
        """Close the S3 connection."""
        self.s3_client = None
        logger.info("Closed S3 connection")


class LocalStorage(CloudStorageBase):
    """Local file system storage implementation for development and testing."""
    
    def __init__(self, base_dir: Union[str, Path]):
        """Initialize local storage.
        
        Args:
            base_dir: Base directory for storage.
        """
        super().__init__()
        self.base_dir = Path(base_dir)
        
    def connect(self) -> bool:
        """Connect to local storage.
        
        Returns:
            bool: True if connection was successful, False otherwise.
        """
        try:
            # Create base directory if it doesn't exist
            os.makedirs(self.base_dir, exist_ok=True)
            
            logger.info(f"Connected to local storage: {self.base_dir}")
            return True
            
        except Exception as e:
            logger.exception(f"Error connecting to local storage: {e}")
            return False
            
    def store_file(
        self,
        local_path: Union[str, Path],
        remote_path: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """Store a local file in local storage.
        
        Args:
            local_path: Path to source file.
            remote_path: Path in local storage.
            metadata: Optional metadata to store with the file.
            
        Returns:
            bool: True if storage was successful, False otherwise.
        """
        try:
            # Ensure paths are Path objects
            local_path = Path(local_path)
            remote_full_path = self.base_dir / remote_path
            
            # Create directory if it doesn't exist
            os.makedirs(remote_full_path.parent, exist_ok=True)
            
            # Copy file
            import shutil
            shutil.copy2(local_path, remote_full_path)
            
            # Store metadata if provided
            if metadata:
                metadata_path = str(remote_full_path) + ".metadata"
                with open(metadata_path, "w") as f:
                    import json
                    json.dump(metadata, f)
                    
            logger.info(f"Stored file in local storage: {remote_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Error storing file in local storage: {e}")
            return False
            
    def store_text(
        self,
        text: str,
        remote_path: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """Store text content in local storage.
        
        Args:
            text: Text content to store.
            remote_path: Path in local storage.
            metadata: Optional metadata to store with the file.
            
        Returns:
            bool: True if storage was successful, False otherwise.
        """
        try:
            # Ensure path is a Path object
            remote_full_path = self.base_dir / remote_path
            
            # Create directory if it doesn't exist
            os.makedirs(remote_full_path.parent, exist_ok=True)
            
            # Write text
            with open(remote_full_path, "w", encoding="utf-8") as f:
                f.write(text)
                
            # Store metadata if provided
            if metadata:
                metadata_path = str(remote_full_path) + ".metadata"
                with open(metadata_path, "w") as f:
                    import json
                    json.dump(metadata, f)
                    
            logger.info(f"Stored text in local storage: {remote_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Error storing text in local storage: {e}")
            return False
            
    def get_file(
        self,
        remote_path: str,
        local_path: Union[str, Path]
    ) -> bool:
        """Download a file from local storage.
        
        Args:
            remote_path: Path in local storage.
            local_path: Path to save the file locally.
            
        Returns:
            bool: True if download was successful, False otherwise.
        """
        try:
            # Ensure paths are Path objects
            remote_full_path = self.base_dir / remote_path
            local_path = Path(local_path)
            
            # Check if file exists
            if not remote_full_path.exists():
                logger.error(f"File not found in local storage: {remote_path}")
                return False
                
            # Create directory if it doesn't exist
            os.makedirs(local_path.parent, exist_ok=True)
            
            # Copy file
            import shutil
            shutil.copy2(remote_full_path, local_path)
            
            logger.info(f"Retrieved file from local storage: {remote_path} to {local_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Error retrieving file from local storage: {e}")
            return False
            
    def get_text(self, remote_path: str) -> Optional[str]:
        """Get text content from local storage.
        
        Args:
            remote_path: Path in local storage.
            
        Returns:
            Optional[str]: Text content, or None if not found.
        """
        try:
            # Ensure path is a Path object
            remote_full_path = self.base_dir / remote_path
            
            # Check if file exists
            if not remote_full_path.exists():
                logger.error(f"File not found in local storage: {remote_path}")
                return None
                
            # Read text
            with open(remote_full_path, "r", encoding="utf-8") as f:
                content = f.read()
                
            logger.info(f"Retrieved text from local storage: {remote_path}")
            return content
            
        except Exception as e:
            logger.exception(f"Error retrieving text from local storage: {e}")
            return None
            
    def list_files(self, prefix: str) -> List[str]:
        """List files in local storage with a given prefix.
        
        Args:
            prefix: Prefix to filter files.
            
        Returns:
            List[str]: List of file paths.
        """
        try:
            # Ensure prefix is a Path object
            prefix_path = self.base_dir / prefix
            
            # Get prefix directory
            prefix_dir = prefix_path.parent
            
            # List files
            files = []
            for root, _, filenames in os.walk(prefix_dir):
                for filename in filenames:
                    # Skip metadata files
                    if filename.endswith(".metadata"):
                        continue
                        
                    # Get full path
                    full_path = Path(root) / filename
                    
                    # Convert to relative path
                    rel_path = full_path.relative_to(self.base_dir)
                    
                    # Check prefix
                    if str(rel_path).startswith(prefix):
                        files.append(str(rel_path))
                        
            logger.info(f"Listed {len(files)} files in local storage with prefix: {prefix}")
            return files
            
        except Exception as e:
            logger.exception(f"Error listing files in local storage: {e}")
            return []
            
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file from local storage.
        
        Args:
            remote_path: Path in local storage.
            
        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        try:
            # Ensure path is a Path object
            remote_full_path = self.base_dir / remote_path
            
            # Check if file exists
            if not remote_full_path.exists():
                logger.error(f"File not found in local storage: {remote_path}")
                return False
                
            # Delete file
            os.remove(remote_full_path)
            
            # Delete metadata if it exists
            metadata_path = str(remote_full_path) + ".metadata"
            if os.path.exists(metadata_path):
                os.remove(metadata_path)
                
            logger.info(f"Deleted file from local storage: {remote_path}")
            return True
            
        except Exception as e:
            logger.exception(f"Error deleting file from local storage: {e}")
            return False
