import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
import json
from dataclasses import dataclass
from typing import List, Dict

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from entity_matching.llm_entity_matcher import LLMEntityMatcher, EntityMatch
from utils.llm_client import LLMClient
from utils.database import DatabaseManager


class TestEntityFiltering:
    """Test suite for entity filtering logic in LLM Entity Matcher"""
    
    @pytest.fixture
    def mock_llm_client(self):
        return Mock(spec=LLMClient)
    
    @pytest.fixture
    def mock_db_manager(self):
        return Mock(spec=DatabaseManager)
    
    @pytest.fixture
    def entity_matcher(self, mock_llm_client, mock_db_manager):
        return LLMEntityMatcher(mock_llm_client, mock_db_manager)
    
    @pytest.fixture
    def sample_cc_record(self):
        return {
            'id': 1,
            'website_url': 'https://example.com.au',
            'company_name': 'Example Tech Solutions',
            'industry': 'Technology',
            'meta_description': 'Leading tech solutions provider',
            'title': 'Example Tech - Home'
        }
    
    @pytest.fixture
    def sample_abr_records(self):
        return [
            {
                'id': 101,
                'abn': '12345678901',
                'entity_name': 'Example Technology Solutions Pty Ltd',
                'entity_status': 'Active',
                'address_state': 'NSW',
                'address_suburb': 'Sydney',
                'address_postcode': '2000',
                'trading_names': ['Example Tech', 'ExampleTech'],
                'business_names': []
            },
            {
                'id': 102,
                'abn': '98765432109',
                'entity_name': 'Different Company Ltd',
                'entity_status': 'Active',
                'address_state': 'VIC',
                'address_suburb': 'Melbourne',
                'address_postcode': '3000',
                'trading_names': [],
                'business_names': ['Different Business']
            },
            {
                'id': 103,
                'abn': '11223344556',
                'entity_name': 'Inactive Company',
                'entity_status': 'Cancelled',
                'address_state': 'QLD',
                'address_suburb': 'Brisbane',
                'address_postcode': '4000',
                'trading_names': [],
                'business_names': []
            }
        ]


class TestCandidateFiltering:
    """Test candidate filtering logic"""
    
    def test_filter_candidates_excludes_inactive_entities(self, entity_matcher, sample_cc_record, sample_abr_records):
        """Test that inactive entities are excluded from candidates"""
        candidates = entity_matcher._filter_candidates(sample_cc_record, sample_abr_records)
        
        # Should exclude the cancelled entity (id: 103)
        candidate_ids = [c['id'] for c in candidates]
        assert 103 not in candidate_ids
        assert len(candidates) <= 2
    
    def test_filter_candidates_domain_name_similarity(self, entity_matcher):
        """Test domain-name similarity filtering"""
        cc_record = {
            'id': 1,
            'website_url': 'https://exampletech.com.au',
            'company_name': 'Some Company'
        }
        
        abr_records = [
            {
                'id': 201,
                'entity_name': 'Example Technology Pty Ltd',
                'entity_status': 'Active',
                'trading_names': [],
                'business_names': []
            },
            {
                'id': 202,
                'entity_name': 'Unrelated Business Ltd',
                'entity_status': 'Active',
                'trading_names': [],
                'business_names': []
            }
        ]
        
        candidates = entity_matcher._filter_candidates(cc_record, abr_records)
        
        # Should include entity with matching domain name
        candidate_ids = [c['id'] for c in candidates]
        assert 201 in candidate_ids
        
    def test_filter_candidates_quick_name_similarity(self, entity_matcher):
        """Test quick name similarity filtering"""
        cc_record = {
            'id': 1,
            'website_url': 'https://different-domain.com.au',
            'company_name': 'Tech Solutions Company'
        }
        
        abr_records = [
            {
                'id': 301,
                'entity_name': 'Technology Solutions Company Pty Ltd',
                'entity_status': 'Active',
                'trading_names': [],
                'business_names': []
            },
            {
                'id': 302,
                'entity_name': 'Completely Different Business',
                'entity_status': 'Active',
                'trading_names': [],
                'business_names': []
            }
        ]
        
        candidates = entity_matcher._filter_candidates(cc_record, abr_records)
        
        # Should include similar name entity
        candidate_ids = [c['id'] for c in candidates]
        assert 301 in candidate_ids
        # Should exclude dissimilar name entity
        assert 302 not in candidate_ids
        
    def test_filter_candidates_trading_names_matching(self, entity_matcher):
        """Test filtering based on trading names"""
        cc_record = {
            'id': 1,
            'website_url': 'https://quicktech.com.au',
            'company_name': 'QuickTech'
        }
        
        abr_records = [
            {
                'id': 401,
                'entity_name': 'Long Official Business Name Pty Ltd',
                'entity_status': 'Active',
                'trading_names': ['QuickTech Solutions', 'QT'],
                'business_names': []
            }
        ]
        
        candidates = entity_matcher._filter_candidates(cc_record, abr_records)
        
        # Should match based on trading name similarity
        candidate_ids = [c['id'] for c in candidates]
        assert 401 in candidate_ids
        
    def test_empty_candidates_for_no_matches(self, entity_matcher):
        """Test that no candidates are returned when no matches found"""
        cc_record = {
            'id': 1,
            'website_url': 'https://uniquecompany.com.au',
            'company_name': 'Unique Company Name'
        }
        
        abr_records = [
            {
                'id': 501,
                'entity_name': 'Completely Different Business Type',
                'entity_status': 'Active',
                'trading_names': ['Other Trading Name'],
                'business_names': []
            }
        ]
        
        candidates = entity_matcher._filter_candidates(cc_record, abr_records)
        
        # Should return empty list
        assert len(candidates) == 0


class TestDomainExtraction:
    """Test domain extraction and comparison logic"""
    
    def test_extract_domain_basic(self, entity_matcher):
        """Test basic domain extraction"""
        url = 'https://example.com.au'
        domain = entity_matcher._extract_domain(url)
        assert domain == 'example'
    
    def test_extract_domain_with_www(self, entity_matcher):
        """Test domain extraction removes www"""
        url = 'https://www.example.com.au'
        domain = entity_matcher._extract_domain(url)
        assert domain == 'example'
    
    def test_extract_domain_different_tlds(self, entity_matcher):
        """Test domain extraction with different Australian TLDs"""
        test_cases = [
            ('https://example.com.au', 'example'),
            ('https://example.net.au', 'example'),
            ('https://example.org.au', 'example'),
            ('https://example.edu.au', 'example'),
            ('https://example.gov.au', 'example'),
        ]
        
        for url, expected in test_cases:
            domain = entity_matcher._extract_domain(url)
            assert domain == expected, f"Failed for URL: {url}"
    
    def test_extract_domain_invalid_url(self, entity_matcher):
        """Test domain extraction with invalid URLs"""
        invalid_urls = ['', 'not-a-url', 'ftp://example.com', None]
        
        for url in invalid_urls:
            domain = entity_matcher._extract_domain(url)
            assert domain is None
    
    def test_domain_name_similarity_exact_match(self, entity_matcher):
        """Test domain name similarity with exact match"""
        domain = 'techsolutions'
        company_name = 'Tech Solutions Pty Ltd'
        
        result = entity_matcher._domain_name_similarity(domain, company_name)
        assert result is True
    
    def test_domain_name_similarity_partial_match(self, entity_matcher):
        """Test domain name similarity with partial match"""
        domain = 'quicktech'
        company_name = 'Quick Technology Solutions'
        
        result = entity_matcher._domain_name_similarity(domain, company_name)
        assert result is True
    
    def test_domain_name_similarity_no_match(self, entity_matcher):
        """Test domain name similarity with no match"""
        domain = 'completely'
        company_name = 'Different Business Name'
        
        result = entity_matcher._domain_name_similarity(domain, company_name)
        assert result is False
    
    def test_domain_name_similarity_short_names(self, entity_matcher):
        """Test domain name similarity rejects very short matches"""
        domain = 'ab'
        company_name = 'AB Testing Company'
        
        result = entity_matcher._domain_name_similarity(domain, company_name)
        assert result is False


class TestConfidenceThresholds:
    """Test confidence threshold filtering"""
    
    def test_manual_review_threshold_filtering(self, entity_matcher, sample_cc_record, sample_abr_records):
        """Test that records below manual review threshold are filtered out"""
        # Mock similarity calculation to return low score
        with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
            mock_calc.return_value = 0.35  # Below manual_review_threshold (0.40)
            
            # Mock the filtering to return candidates
            with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
                mock_filter.return_value = sample_abr_records[:1]
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    matches = loop.run_until_complete(
                        entity_matcher._find_best_matches(sample_cc_record, sample_abr_records)
                    )
                    
                    # Should return no matches due to low similarity
                    assert len(matches) == 0
                finally:
                    loop.close()
    
    def test_llm_review_threshold_triggers_llm(self, entity_matcher, sample_cc_record, sample_abr_records):
        """Test that scores above LLM review threshold trigger LLM verification"""
        # Mock similarity calculation to return score that triggers LLM
        with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
            mock_calc.return_value = 0.75  # Above llm_review_threshold (0.60)
            
            # Mock LLM verification
            with patch.object(entity_matcher, '_llm_verify_match', new_callable=AsyncMock) as mock_llm:
                mock_llm.return_value = {
                    'is_match': True,
                    'confidence': 0.85,
                    'reasoning': 'Strong match based on name similarity'
                }
                
                # Mock the filtering to return candidates
                with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
                    mock_filter.return_value = sample_abr_records[:1]
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        matches = loop.run_until_complete(
                            entity_matcher._find_best_matches(sample_cc_record, sample_abr_records)
                        )
                        
                        # Should call LLM verification
                        mock_llm.assert_called_once()
                        
                        # Should return match
                        assert len(matches) == 1
                        assert matches[0].llm_confidence == 0.85
                    finally:
                        loop.close()


class TestFilteringEfficiencyLimits:
    """Test that filtering implements efficiency limits to control costs"""
    
    def test_candidate_limit_top_50(self, entity_matcher, sample_cc_record):
        """Test that only top 50 candidates are processed for similarity"""
        # Create 100 ABR records
        large_abr_set = []
        for i in range(100):
            large_abr_set.append({
                'id': i,
                'entity_name': f'Company {i:03d} Pty Ltd',
                'entity_status': 'Active',
                'trading_names': [],
                'business_names': []
            })
        
        # Mock filtering to return all candidates
        with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
            mock_filter.return_value = large_abr_set
            
            # Mock similarity calculation to count calls
            with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
                mock_calc.return_value = 0.30  # Below threshold
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    matches = loop.run_until_complete(
                        entity_matcher._find_best_matches(sample_cc_record, large_abr_set)
                    )
                    
                    # Should only call similarity calculation 50 times (top 50 limit)
                    assert mock_calc.call_count == 50
                finally:
                    loop.close()
    
    def test_llm_review_limit_top_5(self, entity_matcher, sample_cc_record):
        """Test that only top 5 candidates are sent for LLM review"""
        # Create candidates that will pass similarity threshold
        candidates = []
        for i in range(10):
            candidates.append({
                'id': i,
                'entity_name': f'Similar Company {i:02d}',
                'entity_status': 'Active',
                'trading_names': [],
                'business_names': []
            })
        
        # Mock filtering and similarity to return high scores
        with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
            mock_filter.return_value = candidates
            
            with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
                mock_calc.return_value = 0.75  # Above LLM threshold
                
                # Mock LLM verification
                with patch.object(entity_matcher, '_llm_verify_match', new_callable=AsyncMock) as mock_llm:
                    mock_llm.return_value = {
                        'is_match': False,
                        'confidence': 0.70,
                        'reasoning': 'Not a strong enough match'
                    }
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        matches = loop.run_until_complete(
                            entity_matcher._find_best_matches(sample_cc_record, candidates)
                        )
                        
                        # Should only call LLM verification 5 times (top 5 limit)
                        assert mock_llm.call_count == 5
                    finally:
                        loop.close()
    
    def test_early_termination_on_first_match(self, entity_matcher, sample_cc_record):
        """Test that matching stops on first confirmed match"""
        candidates = []
        for i in range(5):
            candidates.append({
                'id': i,
                'entity_name': f'Match Candidate {i:02d}',
                'entity_status': 'Active',
                'trading_names': [],
                'business_names': []
            })
        
        # Mock filtering and similarity
        with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
            mock_filter.return_value = candidates
            
            with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
                mock_calc.return_value = 0.75
                
                # Mock LLM verification to return match on first call
                call_count = 0
                async def mock_llm_response(*args, **kwargs):
                    nonlocal call_count
                    call_count += 1
                    if call_count == 1:
                        return {
                            'is_match': True,
                            'confidence': 0.85,
                            'reasoning': 'Strong match found'
                        }
                    else:
                        return {
                            'is_match': False,
                            'confidence': 0.60,
                            'reasoning': 'Not a match'
                        }
                
                with patch.object(entity_matcher, '_llm_verify_match', new_callable=AsyncMock) as mock_llm:
                    mock_llm.side_effect = mock_llm_response
                    
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        matches = loop.run_until_complete(
                            entity_matcher._find_best_matches(sample_cc_record, candidates)
                        )
                        
                        # Should only call LLM once and stop
                        assert mock_llm.call_count == 1
                        
                        # Should return exactly one match
                        assert len(matches) == 1
                        assert matches[0].llm_confidence == 0.85
                    finally:
                        loop.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])