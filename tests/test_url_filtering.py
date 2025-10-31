import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import asyncio
import sys
import os
from typing import List, Dict

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from extractors.common_crawl_extractor import CommonCrawlExtractor, CompanyWebsiteData
from utils.llm_client import LLMClient
from utils.database import DatabaseManager


class TestURLFiltering:
    """Test suite for URL filtering logic in Common Crawl extractor"""
    
    @pytest.fixture
    def mock_llm_client(self):
        return Mock(spec=LLMClient)
    
    @pytest.fixture
    def mock_db_manager(self):
        return Mock(spec=DatabaseManager)
    
    @pytest.fixture
    def extractor(self, mock_llm_client, mock_db_manager):
        return CommonCrawlExtractor(mock_llm_client, mock_db_manager)


class TestCompanyURLIdentification:
    """Test identification of likely company URLs"""
    
    def test_is_likely_company_url_valid_business_domains(self, extractor):
        """Test that valid business domains are identified as company URLs"""
        valid_urls = [
            'https://techsolutions.com.au',
            'https://www.australianbusiness.net.au',
            'https://consulting.org.au',
            'https://legal-services.asn.au',
            'https://manufacturing.edu.au',
            'https://government-contractor.gov.au'
        ]
        
        for url in valid_urls:
            assert extractor._is_likely_company_url(url), f"Failed to identify {url} as company URL"
    
    def test_is_likely_company_url_excludes_blog_paths(self, extractor):
        """Test that blog and news URLs are excluded"""
        excluded_urls = [
            'https://company.com.au/blog/latest-news',
            'https://business.net.au/news/industry-update',
            'https://website.org.au/articles/guide',
            'https://site.com.au/wp-content/uploads/image.jpg',
            'https://company.asn.au/wp-admin/dashboard'
        ]
        
        for url in excluded_urls:
            assert not extractor._is_likely_company_url(url), f"Should have excluded {url}"
    
    def test_is_likely_company_url_excludes_file_downloads(self, extractor):
        """Test that file download URLs are excluded"""
        file_urls = [
            'https://company.com.au/documents/report.pdf',
            'https://business.net.au/files/presentation.doc',
            'https://website.org.au/images/logo.jpg',
            'https://site.com.au/downloads/software.zip',
            'https://company.asn.au/files/installer.exe'
        ]
        
        for url in file_urls:
            assert not extractor._is_likely_company_url(url), f"Should have excluded file URL {url}"
    
    def test_is_likely_company_url_excludes_user_content(self, extractor):
        """Test that user-generated content URLs are excluded"""
        user_urls = [
            'https://company.com.au/user/profile/123',
            'https://business.net.au/member/dashboard',
            'https://website.org.au/profile/john-smith',
            'https://site.com.au/users/login'
        ]
        
        for url in user_urls:
            assert not extractor._is_likely_company_url(url), f"Should have excluded user URL {url}"
    
    def test_is_likely_company_url_non_australian_domains(self, extractor):
        """Test that non-Australian domains are excluded"""
        non_au_urls = [
            'https://company.com',
            'https://business.co.uk',
            'https://website.org',
            'https://site.net',
            'https://example.de'
        ]
        
        for url in non_au_urls:
            assert not extractor._is_likely_company_url(url), f"Should have excluded non-AU URL {url}"
    
    def test_is_likely_company_url_invalid_schemes(self, extractor):
        """Test that non-HTTP(S) URLs are excluded"""
        invalid_scheme_urls = [
            'ftp://company.com.au/files',
            'file:///local/path/file.html',
            'mailto:contact@company.com.au',
            'tel:+61-2-9999-9999'
        ]
        
        for url in invalid_scheme_urls:
            assert not extractor._is_likely_company_url(url), f"Should have excluded invalid scheme URL {url}"
    
    def test_is_likely_company_url_malformed_urls(self, extractor):
        """Test that malformed URLs are excluded"""
        malformed_urls = [
            'not-a-url',
            'https://',
            'https://.',
            '',
            None
        ]
        
        for url in malformed_urls:
            assert not extractor._is_likely_company_url(url), f"Should have excluded malformed URL {url}"


class TestDomainClassification:
    """Test domain classification logic"""
    
    def test_australian_domain_identification(self, extractor):
        """Test identification of Australian domains"""
        au_domains = [
            'company.com.au',
            'business.net.au',
            'organization.org.au',
            'school.edu.au',
            'government.gov.au',
            'association.asn.au'
        ]
        
        for domain in au_domains:
            url = f'https://{domain}'
            assert extractor._is_likely_company_url(url), f"Should identify {domain} as Australian"
    
    def test_subdomain_handling(self, extractor):
        """Test that subdomains are handled correctly"""
        subdomain_urls = [
            'https://www.company.com.au',
            'https://shop.business.net.au',
            'https://blog.organization.org.au',  # Should be excluded due to 'blog'
            'https://api.service.com.au',
            'https://secure.banking.com.au'
        ]
        
        expected_results = [True, True, False, True, True]
        
        for url, expected in zip(subdomain_urls, expected_results):
            result = extractor._is_likely_company_url(url)
            assert result == expected, f"Incorrect result for {url}: got {result}, expected {expected}"


class TestPathExclusions:
    """Test specific path exclusion patterns"""
    
    def test_wordpress_path_exclusions(self, extractor):
        """Test that WordPress-specific paths are excluded"""
        wp_urls = [
            'https://company.com.au/wp-admin/edit.php',
            'https://business.net.au/wp-content/themes/default/style.css',
            'https://website.org.au/wp-includes/js/script.js',
            'https://site.com.au/wp-json/api/posts'
        ]
        
        for url in wp_urls:
            assert not extractor._is_likely_company_url(url), f"Should exclude WordPress path {url}"
    
    def test_common_cms_path_exclusions(self, extractor):
        """Test that common CMS paths are excluded"""
        cms_urls = [
            'https://company.com.au/admin/dashboard',
            'https://business.net.au/administrator/index.php',
            'https://website.org.au/cms/login',
            'https://site.com.au/backend/users'
        ]
        
        for url in cms_urls:
            assert not extractor._is_likely_company_url(url), f"Should exclude CMS path {url}"
    
    def test_api_endpoint_exclusions(self, extractor):
        """Test that API endpoints are excluded"""
        api_urls = [
            'https://company.com.au/api/v1/users',
            'https://business.net.au/rest/products',
            'https://website.org.au/graphql/query',
            'https://site.com.au/webhook/payment'
        ]
        
        for url in api_urls:
            assert not extractor._is_likely_company_url(url), f"Should exclude API endpoint {url}"


class TestURLNormalization:
    """Test URL normalization and standardization"""
    
    def test_url_normalization_removes_fragments(self, extractor):
        """Test that URL fragments are handled appropriately"""
        urls_with_fragments = [
            'https://company.com.au/about#team',
            'https://business.net.au/services#pricing',
            'https://website.org.au/contact#location'
        ]
        
        # These should still be considered company URLs despite fragments
        for url in urls_with_fragments:
            assert extractor._is_likely_company_url(url), f"Should accept URL with fragment {url}"
    
    def test_url_normalization_handles_query_params(self, extractor):
        """Test that query parameters are handled appropriately"""
        urls_with_params = [
            'https://company.com.au/products?category=tech',
            'https://business.net.au/search?q=services',
            'https://website.org.au/page?id=123'
        ]
        
        # These should still be considered company URLs despite query params
        for url in urls_with_params:
            assert extractor._is_likely_company_url(url), f"Should accept URL with query params {url}"
    
    def test_case_insensitive_filtering(self, extractor):
        """Test that URL filtering is case insensitive"""
        case_variants = [
            'https://Company.Com.Au/About',
            'https://BUSINESS.NET.AU/SERVICES',
            'https://Website.Org.Au/Contact'
        ]
        
        for url in case_variants:
            assert extractor._is_likely_company_url(url), f"Should handle case variant {url}"


class TestURLFilteringPerformance:
    """Test performance aspects of URL filtering"""
    
    def test_batch_url_filtering(self, extractor):
        """Test that batch URL filtering works efficiently"""
        # Generate a large set of mixed URLs
        test_urls = []
        
        # Add valid company URLs
        for i in range(100):
            test_urls.append(f'https://company{i:03d}.com.au')
        
        # Add URLs that should be filtered out
        for i in range(50):
            test_urls.append(f'https://company{i:03d}.com.au/blog/post-{i}')
            test_urls.append(f'https://company{i:03d}.com.au/wp-admin/edit.php')
            test_urls.append(f'https://company{i:03d}.com.au/files/document{i}.pdf')
        
        # Filter URLs
        valid_urls = [url for url in test_urls if extractor._is_likely_company_url(url)]
        
        # Should have filtered out the blog, wp-admin, and file URLs
        assert len(valid_urls) == 100, f"Expected 100 valid URLs, got {len(valid_urls)}"
        
        # Check that all valid URLs are company homepage-style URLs
        for url in valid_urls:
            assert '/blog/' not in url
            assert '/wp-admin/' not in url
            assert '.pdf' not in url
    
    def test_url_filtering_edge_cases(self, extractor):
        """Test edge cases in URL filtering"""
        edge_case_urls = [
            'https://company.com.au/',  # Trailing slash
            'https://company.com.au',   # No trailing slash
            'https://www.company.com.au/index.html',  # Index page
            'https://company.com.au/home',  # Home page
            'https://company.com.au/index.php',  # PHP index
        ]
        
        # All of these should be considered valid company URLs
        for url in edge_case_urls:
            assert extractor._is_likely_company_url(url), f"Should accept edge case URL {url}"


class TestCommonCrawlURLExtraction:
    """Test URL extraction from Common Crawl index"""
    
    @pytest.mark.asyncio
    async def test_url_extraction_with_filtering(self, extractor, mock_db_manager):
        """Test that URL extraction applies filtering"""
        # Mock the database to return a mix of URLs
        mock_urls = [
            {'url': 'https://goodcompany.com.au', 'urlkey': 'au,com,goodcompany)/'},
            {'url': 'https://anotherbiz.net.au/about', 'urlkey': 'au,net,anotherbiz)/about'},
            {'url': 'https://company.com.au/blog/news', 'urlkey': 'au,com,company)/blog/news'},  # Should be filtered
            {'url': 'https://business.org.au/wp-admin/index.php', 'urlkey': 'au,org,business)/wp-admin/index.php'},  # Should be filtered
            {'url': 'https://website.com.au/files/doc.pdf', 'urlkey': 'au,com,website)/files/doc.pdf'},  # Should be filtered
        ]
        
        mock_db_manager.fetch_all = AsyncMock(return_value=mock_urls)
        extractor.db_manager = mock_db_manager
        
        # Mock the batch processing to avoid actual LLM calls
        with patch.object(extractor, '_process_batch_with_llm', new_callable=AsyncMock) as mock_process:
            mock_process.return_value = []
            
            await extractor.extract_companies(max_records=100)
            
            # Verify that only valid URLs were processed
            call_args = mock_process.call_args[0][0]  # First argument (urls)
            
            # Should have filtered out blog, wp-admin, and PDF URLs
            assert len(call_args) == 2
            assert any('goodcompany' in url['url'] for url in call_args)
            assert any('anotherbiz' in url['url'] for url in call_args)
            assert not any('blog' in url['url'] for url in call_args)
            assert not any('wp-admin' in url['url'] for url in call_args)
            assert not any('.pdf' in url['url'] for url in call_args)
    
    def test_url_deduplication(self, extractor):
        """Test that duplicate URLs are handled appropriately"""
        duplicate_urls = [
            'https://company.com.au',
            'https://company.com.au/',
            'https://www.company.com.au',
            'https://www.company.com.au/',
        ]
        
        # All should be considered valid, but in practice deduplication 
        # would happen at the database/query level
        for url in duplicate_urls:
            assert extractor._is_likely_company_url(url)


class TestURLFilteringIntegration:
    """Test integration of URL filtering with extraction process"""
    
    @pytest.mark.asyncio
    async def test_end_to_end_url_filtering(self, extractor, mock_db_manager, mock_llm_client):
        """Test complete URL filtering pipeline"""
        # Mock database response with mixed URLs
        mock_cc_response = [
            {'url': 'https://realcompany.com.au', 'urlkey': 'au,com,realcompany)/'},
            {'url': 'https://business.net.au/services', 'urlkey': 'au,net,business)/services'},
            {'url': 'https://company.com.au/blog/post1', 'urlkey': 'au,com,company)/blog/post1'},
            {'url': 'https://site.org.au/wp-admin/', 'urlkey': 'au,org,site)/wp-admin/'},
        ]
        
        mock_db_manager.fetch_all = AsyncMock(return_value=mock_cc_response)
        extractor.db_manager = mock_db_manager
        
        # Mock LLM responses
        mock_llm_client.batch_completions = AsyncMock(return_value=[
            '{"company_name": "Real Company", "industry": "Technology", "confidence": 0.9}',
            '{"company_name": "Business Services", "industry": "Consulting", "confidence": 0.85}'
        ])
        extractor.llm_client = mock_llm_client
        
        # Mock database insert
        mock_db_manager.bulk_insert = AsyncMock()
        
        # Run extraction
        result = await extractor.extract_companies(max_records=10)
        
        # Verify that only 2 valid URLs were processed (filtered out blog and wp-admin)
        assert mock_llm_client.batch_completions.call_count > 0
        
        # Check that batch_completions was called with only valid URLs
        call_args = mock_llm_client.batch_completions.call_args[0][0]  # Prompts
        assert len(call_args) == 2  # Only 2 valid URLs should have been processed


class TestURLFilteringConfiguration:
    """Test configuration aspects of URL filtering"""
    
    def test_excluded_paths_configuration(self, extractor):
        """Test that excluded paths can be configured"""
        # Test that the extractor has configurable excluded paths
        assert hasattr(extractor, '_is_likely_company_url')
        
        # Test some specific exclusions
        excluded_urls = [
            'https://company.com.au/blog/article',
            'https://company.com.au/news/update',
            'https://company.com.au/articles/guide',
            'https://company.com.au/wp-content/file.css',
            'https://company.com.au/wp-admin/dashboard',
        ]
        
        for url in excluded_urls:
            assert not extractor._is_likely_company_url(url)
    
    def test_excluded_extensions_configuration(self, extractor):
        """Test that excluded file extensions can be configured"""
        file_extensions = ['.pdf', '.doc', '.jpg', '.png', '.zip', '.exe']
        
        for ext in file_extensions:
            url = f'https://company.com.au/files/document{ext}'
            assert not extractor._is_likely_company_url(url), f"Should exclude {ext} files"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])