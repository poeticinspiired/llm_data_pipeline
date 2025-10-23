import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration class for the LLM data pipeline."""
    
    # Data source configuration
    JSONL_PATH = os.getenv('JSONL_PATH', 'data/default.jsonl')
    TEXT_FIELD = os.getenv('TEXT_FIELD', 'text')
    ID_FIELD = os.getenv('ID_FIELD', 'id')
    LIMIT = int(os.getenv('LIMIT', '1000'))
    DATASET_VERSION = os.getenv('DATASET_VERSION', 'v1.0')
    
    # Optional: Database configuration
    DATABASE_URL = os.getenv('DATABASE_URL')
    MONGODB_URI = os.getenv('MONGODB_URI')
    
    # Optional: API Keys
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
    
    @classmethod
    def validate(cls):
        """Validate required configuration."""
        if not cls.JSONL_PATH:
            raise ValueError("JSONL_PATH is required")
        return True

config = Config()