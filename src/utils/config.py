"""
Configuration management for the Australian company pipeline.
Handles environment variables, settings, and configuration validation.
"""

import os
from typing import Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path
import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str
    port: int
    database: str
    username: str
    password: str
    pool_size: int = 20
    
    @property
    def url(self) -> str:
        """Get database connection URL."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class LLMConfig:
    """LLM configuration."""
    provider: str = "openai"
    model: str = "gpt-4-turbo-preview"
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    temperature: float = 0.3
    max_tokens: int = 2000


@dataclass
class ExtractorConfig:
    """Data extraction configuration."""
    common_crawl_max_records: int = 200000
    abr_max_records: int = 1000000
    batch_size: int = 1000
    download_dir: str = "./data"
    concurrent_requests: int = 10


@dataclass
class EntityMatchingConfig:
    """Entity matching configuration."""
    exact_match_threshold: float = 0.95
    high_confidence_threshold: float = 0.85
    llm_review_threshold: float = 0.60
    manual_review_threshold: float = 0.40
    batch_size: int = 1000


class Config:
    """
    Main configuration class for the Australian company pipeline.
    Loads configuration from environment variables, config files, and defaults.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_file: Optional path to YAML config file
        """
        self._config_data = {}
        
        # Load from config file if provided
        if config_file and Path(config_file).exists():
            with open(config_file, 'r') as f:
                self._config_data = yaml.safe_load(f) or {}
        
        # Initialize configuration sections
        self.database = self._init_database_config()
        self.llm = self._init_llm_config()
        self.extractor = self._init_extractor_config()
        self.entity_matching = self._init_entity_matching_config()
        
        # Validate configuration
        self._validate_config()
    
    def _init_database_config(self) -> DatabaseConfig:
        """Initialize database configuration."""
        db_config = self._config_data.get('database', {})
        
        return DatabaseConfig(
            host=self._get_value('DATABASE_HOST', 'db.host', 'localhost'),
            port=int(self._get_value('DATABASE_PORT', 'db.port', 5432)),
            database=self._get_value('DATABASE_NAME', 'db.database', 'australian_companies'),
            username=self._get_value('DATABASE_USER', 'db.username', 'postgres'),
            password=self._get_value('DATABASE_PASSWORD', 'db.password', ''),
            pool_size=int(self._get_value('DATABASE_POOL_SIZE', 'db.pool_size', 20))
        )
    
    def _init_llm_config(self) -> LLMConfig:
        """Initialize LLM configuration."""
        llm_config = self._config_data.get('llm', {})
        
        return LLMConfig(
            provider=self._get_value('LLM_PROVIDER', 'llm.provider', 'openai'),
            model=self._get_value('LLM_MODEL', 'llm.model', 'gpt-4-turbo-preview'),
            openai_api_key=self._get_value('OPENAI_API_KEY', 'llm.openai_api_key'),
            anthropic_api_key=self._get_value('ANTHROPIC_API_KEY', 'llm.anthropic_api_key'),
            temperature=float(self._get_value('LLM_TEMPERATURE', 'llm.temperature', 0.3)),
            max_tokens=int(self._get_value('LLM_MAX_TOKENS', 'llm.max_tokens', 2000))
        )
    
    def _init_extractor_config(self) -> ExtractorConfig:
        """Initialize extractor configuration."""
        return ExtractorConfig(
            common_crawl_max_records=int(self._get_value('CC_MAX_RECORDS', 'extractor.common_crawl_max_records', 200000)),
            abr_max_records=int(self._get_value('ABR_MAX_RECORDS', 'extractor.abr_max_records', 1000000)),
            batch_size=int(self._get_value('EXTRACT_BATCH_SIZE', 'extractor.batch_size', 1000)),
            download_dir=self._get_value('DOWNLOAD_DIR', 'extractor.download_dir', './data'),
            concurrent_requests=int(self._get_value('CONCURRENT_REQUESTS', 'extractor.concurrent_requests', 10))
        )
    
    def _init_entity_matching_config(self) -> EntityMatchingConfig:
        """Initialize entity matching configuration."""
        return EntityMatchingConfig(
            exact_match_threshold=float(self._get_value('EXACT_MATCH_THRESHOLD', 'entity_matching.exact_match_threshold', 0.95)),
            high_confidence_threshold=float(self._get_value('HIGH_CONFIDENCE_THRESHOLD', 'entity_matching.high_confidence_threshold', 0.85)),
            llm_review_threshold=float(self._get_value('LLM_REVIEW_THRESHOLD', 'entity_matching.llm_review_threshold', 0.60)),
            manual_review_threshold=float(self._get_value('MANUAL_REVIEW_THRESHOLD', 'entity_matching.manual_review_threshold', 0.40)),
            batch_size=int(self._get_value('MATCHING_BATCH_SIZE', 'entity_matching.batch_size', 1000))
        )
    
    def _get_value(self, env_key: str, config_path: str = None, default: Any = None) -> Any:
        """
        Get configuration value from environment variable or config file.
        
        Args:
            env_key: Environment variable name
            config_path: Dot-separated path in config file (e.g., 'db.host')
            default: Default value if not found
            
        Returns:
            Configuration value
        """
        # First try environment variable
        env_value = os.getenv(env_key)
        if env_value is not None:
            return env_value
        
        # Then try config file
        if config_path:
            keys = config_path.split('.')
            value = self._config_data
            
            try:
                for key in keys:
                    value = value[key]
                return value
            except (KeyError, TypeError):
                pass
        
        return default
    
    def _validate_config(self):
        """Validate configuration and log warnings for missing values."""
        import logging
        logger = logging.getLogger(__name__)
        
        # Check required database settings
        if not self.database.password and self.database.host != 'localhost':
            logger.warning("Database password not set - using empty password")
        
        # Check LLM configuration
        if not self.llm.openai_api_key and not self.llm.anthropic_api_key:
            logger.warning("No LLM API keys configured - will use mock responses")
        
        # Validate thresholds
        if self.entity_matching.exact_match_threshold <= self.entity_matching.high_confidence_threshold:
            logger.warning("Exact match threshold should be higher than high confidence threshold")
    
    @property
    def database_url(self) -> str:
        """Get database connection URL."""
        return self.database.url
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary (excluding sensitive data)."""
        return {
            'database': {
                'host': self.database.host,
                'port': self.database.port,
                'database': self.database.database,
                'username': self.database.username,
                'pool_size': self.database.pool_size
            },
            'llm': {
                'provider': self.llm.provider,
                'model': self.llm.model,
                'temperature': self.llm.temperature,
                'max_tokens': self.llm.max_tokens,
                'has_openai_key': bool(self.llm.openai_api_key),
                'has_anthropic_key': bool(self.llm.anthropic_api_key)
            },
            'extractor': {
                'common_crawl_max_records': self.extractor.common_crawl_max_records,
                'abr_max_records': self.extractor.abr_max_records,
                'batch_size': self.extractor.batch_size,
                'download_dir': self.extractor.download_dir,
                'concurrent_requests': self.extractor.concurrent_requests
            },
            'entity_matching': {
                'exact_match_threshold': self.entity_matching.exact_match_threshold,
                'high_confidence_threshold': self.entity_matching.high_confidence_threshold,
                'llm_review_threshold': self.entity_matching.llm_review_threshold,
                'manual_review_threshold': self.entity_matching.manual_review_threshold,
                'batch_size': self.entity_matching.batch_size
            }
        }
    
    def save_to_file(self, file_path: str):
        """
        Save current configuration to YAML file (excluding sensitive data).
        
        Args:
            file_path: Path to save configuration file
        """
        with open(file_path, 'w') as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)


# Create example configuration files
def create_example_config():
    """Create example configuration files."""
    
    # Create example .env file
    env_content = """# Database Configuration
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=australian_companies
DATABASE_USER=postgres
DATABASE_PASSWORD=your_password_here
DATABASE_POOL_SIZE=20

# LLM Configuration
LLM_PROVIDER=openai
LLM_MODEL=gpt-4-turbo-preview
OPENAI_API_KEY=your_openai_api_key_here
# ANTHROPIC_API_KEY=your_anthropic_api_key_here
LLM_TEMPERATURE=0.3
LLM_MAX_TOKENS=2000

# Extraction Configuration
CC_MAX_RECORDS=200000
ABR_MAX_RECORDS=1000000
EXTRACT_BATCH_SIZE=1000
DOWNLOAD_DIR=./data
CONCURRENT_REQUESTS=10

# Entity Matching Configuration
EXACT_MATCH_THRESHOLD=0.95
HIGH_CONFIDENCE_THRESHOLD=0.85
LLM_REVIEW_THRESHOLD=0.60
MANUAL_REVIEW_THRESHOLD=0.40
MATCHING_BATCH_SIZE=1000
"""
    
    with open('.env.example', 'w') as f:
        f.write(env_content)
    
    # Create example YAML config
    yaml_content = {
        'database': {
            'host': 'localhost',
            'port': 5432,
            'database': 'australian_companies',
            'username': 'postgres',
            'pool_size': 20
        },
        'llm': {
            'provider': 'openai',
            'model': 'gpt-4-turbo-preview',
            'temperature': 0.3,
            'max_tokens': 2000
        },
        'extractor': {
            'common_crawl_max_records': 200000,
            'abr_max_records': 1000000,
            'batch_size': 1000,
            'download_dir': './data',
            'concurrent_requests': 10
        },
        'entity_matching': {
            'exact_match_threshold': 0.95,
            'high_confidence_threshold': 0.85,
            'llm_review_threshold': 0.60,
            'manual_review_threshold': 0.40,
            'batch_size': 1000
        }
    }
    
    with open('config.example.yaml', 'w') as f:
        yaml.dump(yaml_content, f, default_flow_style=False, indent=2)


if __name__ == "__main__":
    # Create example configuration files
    create_example_config()
    print("Created example configuration files: .env.example and config.example.yaml")
    
    # Test configuration loading
    config = Config()
    print("Configuration loaded:")
    print(f"Database URL: {config.database_url}")
    print(f"LLM Provider: {config.llm.provider}")
    print(f"Max Common Crawl Records: {config.extractor.common_crawl_max_records}")