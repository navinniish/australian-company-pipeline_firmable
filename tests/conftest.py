import pytest
import asyncio
import sys
import os
from unittest.mock import Mock, AsyncMock

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_config():
    """Mock configuration for tests."""
    config = Mock()
    config.database_url = "postgresql://test:test@localhost:5432/test_db"
    config.llm_provider = "anthropic"
    config.llm_model = "claude-3-5-haiku-20241022"
    config.llm_api_key = "test-api-key"
    config.common_crawl_max_records = 1000
    config.abr_max_records = 10000
    config.entity_matching_batch_size = 100
    return config

@pytest.fixture
def sample_company_data():
    """Sample company data for testing."""
    return {
        'cc_record': {
            'id': 123,
            'website_url': 'https://techsolutions.com.au',
            'company_name': 'Tech Solutions Australia',
            'industry': 'Technology',
            'meta_description': 'Leading provider of innovative technology solutions',
            'title': 'Tech Solutions - Innovation Partners',
            'extraction_confidence': 0.85
        },
        'abr_record': {
            'id': 456,
            'abn': '12345678901',
            'entity_name': 'Technology Solutions Australia Pty Ltd',
            'entity_status': 'Active',
            'address_suburb': 'Sydney',
            'address_state': 'NSW',
            'address_postcode': '2000',
            'trading_names': ['Tech Solutions', 'TSA'],
            'business_names': ['Tech Solutions Group']
        },
        'entity_match': {
            'common_crawl_id': 123,
            'abr_id': 456,
            'similarity_score': 0.85,
            'matching_method': 'hybrid_llm',
            'llm_confidence': 0.82,
            'llm_reasoning': 'Strong match based on name similarity and domain alignment',
            'manual_review_required': False
        }
    }

@pytest.fixture
def sample_urls():
    """Sample URLs for testing URL filtering."""
    return {
        'valid_urls': [
            'https://techsolutions.com.au',
            'https://www.australianbusiness.net.au',
            'https://consulting.org.au/about',
            'https://manufacturing.asn.au/services',
            'https://legal-services.edu.au/contact'
        ],
        'invalid_urls': [
            'https://company.com.au/blog/latest-news',
            'https://business.net.au/wp-admin/dashboard',
            'https://website.org.au/wp-content/uploads/image.jpg',
            'https://site.com.au/user/profile/123',
            'https://example.com.au/files/document.pdf',
            'https://company.com',  # Non-Australian domain
            'ftp://company.com.au/files'  # Non-HTTP scheme
        ]
    }

@pytest.fixture
def mock_llm_responses():
    """Mock LLM responses for different scenarios."""
    return {
        'extraction_response': '{"company_name": "Tech Solutions Australia", "industry": "Technology", "contact_info": {"email": "info@techsolutions.com.au", "phone": "(02) 9999-8888"}, "confidence": 0.85}',
        
        'entity_match_positive': '{"is_match": true, "confidence": 0.88, "reasoning": "Strong match - company names are highly similar, domain aligns with business name", "key_factors": ["name_similarity", "domain_alignment"]}',
        
        'entity_match_negative': '{"is_match": false, "confidence": 0.25, "reasoning": "Significant differences in company names and industries suggest different entities", "key_factors": ["name_mismatch", "industry_different"]}',
        
        'name_determination': '{"best_name": "Tech Solutions Australia", "reasoning": "Most clear and commonly used name that balances brevity and clarity", "confidence": 0.90}',
        
        'industry_classification': '{"industry": "Information Technology", "reasoning": "Clear technology focus based on services and description", "confidence": 0.85}',
        
        'invalid_json': '{ invalid json response',
        
        'missing_fields': '{"confidence": 0.80, "reasoning": "Missing is_match field"}'
    }

@pytest.fixture
def performance_test_data():
    """Large datasets for performance testing."""
    def generate_urls(count, pattern):
        return [
            {
                'url': f'https://company{i:04d}.com.au{pattern}',
                'urlkey': f'au,com,company{i:04d}){pattern}',
                'charset': 'UTF-8'
            }
            for i in range(count)
        ]
    
    return {
        'valid_company_urls': generate_urls(500, ''),
        'blog_urls': generate_urls(200, '/blog/post'),
        'admin_urls': generate_urls(200, '/wp-admin/'),
        'file_urls': generate_urls(100, '/files/doc.pdf'),
        'large_abr_dataset': [
            {
                'id': 10000 + i,
                'abn': f'1234567{i:04d}',
                'entity_name': f'Business Entity {i:04d} Pty Ltd',
                'entity_status': 'Active' if i % 10 != 0 else 'Cancelled',
                'trading_names': f'["Entity{i:04d}"]' if i % 3 == 0 else '[]',
                'business_names': '[]',
                'address_suburb': 'Sydney',
                'address_state': 'NSW',
                'address_postcode': '2000'
            }
            for i in range(1000)
        ]
    }

# Helper functions for tests
def create_mock_llm_client():
    """Create a mock LLM client with common methods."""
    client = Mock()
    client.chat_completion = AsyncMock()
    client.batch_completions = AsyncMock()
    client.estimate_cost = Mock(return_value=0.01)
    client.estimate_tokens = Mock(return_value=100)
    return client

def create_mock_db_manager():
    """Create a mock database manager with common methods."""
    db = Mock()
    db.fetch_all = AsyncMock()
    db.fetch_one = AsyncMock()
    db.bulk_insert = AsyncMock()
    db.execute_query = AsyncMock()
    db.get_connection = AsyncMock()
    return db

# Test utilities
def assert_similarity_score_valid(score):
    """Assert that a similarity score is valid (between 0 and 1)."""
    assert isinstance(score, (int, float)), f"Score should be numeric, got {type(score)}"
    assert 0.0 <= score <= 1.0, f"Score should be between 0 and 1, got {score}"

def assert_company_name_valid(name):
    """Assert that a company name is valid."""
    assert isinstance(name, str), f"Company name should be string, got {type(name)}"
    assert len(name.strip()) > 0, "Company name should not be empty"
    assert len(name) <= 200, f"Company name too long: {len(name)} characters"

def assert_url_is_australian(url):
    """Assert that a URL is from an Australian domain."""
    australian_tlds = ['.com.au', '.net.au', '.org.au', '.edu.au', '.gov.au', '.asn.au']
    assert any(tld in url.lower() for tld in australian_tlds), f"URL should be Australian: {url}"

def assert_abn_valid_format(abn):
    """Assert that an ABN has valid format (11 digits)."""
    assert isinstance(abn, str), f"ABN should be string, got {type(abn)}"
    assert len(abn) == 11, f"ABN should be 11 digits, got {len(abn)}"
    assert abn.isdigit(), f"ABN should be numeric, got {abn}"