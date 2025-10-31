import pytest
import asyncio
import numpy as np
from unittest.mock import Mock, AsyncMock, patch
import sys
import os
from typing import Dict, List

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from entity_matching.llm_entity_matcher import LLMEntityMatcher
from utils.llm_client import LLMClient
from utils.database import DatabaseManager
from utils.text_processing import normalize_company_name


class TestSimilarityCalculations:
    """Test suite for similarity calculation methods in entity matching"""
    
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
            'website_url': 'https://techsolutions.com.au',
            'company_name': 'Tech Solutions Australia',
            'industry': 'Technology',
            'meta_description': 'Leading provider of innovative technology solutions for businesses',
            'title': 'Tech Solutions - Innovation Partners'
        }
    
    @pytest.fixture
    def sample_abr_record(self):
        return {
            'id': 101,
            'abn': '12345678901',
            'entity_name': 'Technology Solutions Australia Pty Ltd',
            'trading_names': ['Tech Solutions', 'TSA'],
            'business_names': ['Tech Solutions Group'],
            'address_suburb': 'Sydney',
            'address_state': 'NSW',
            'address_postcode': '2000',
            'entity_status': 'Active'
        }


class TestNameSimilarityCalculation:
    """Test name similarity calculation methods"""
    
    def test_calculate_name_similarity_exact_match(self, entity_matcher):
        """Test name similarity with exact matches"""
        name1 = "Tech Solutions Australia"
        name2 = "Tech Solutions Australia"
        
        similarity = entity_matcher._calculate_name_similarity(name1, name2)
        assert similarity == 1.0
    
    def test_calculate_name_similarity_high_similarity(self, entity_matcher):
        """Test name similarity with high similarity names"""
        name1 = "Tech Solutions Australia"
        name2 = "Technology Solutions Australia Pty Ltd"
        
        similarity = entity_matcher._calculate_name_similarity(name1, name2)
        assert similarity > 0.8
        assert similarity < 1.0
    
    def test_calculate_name_similarity_medium_similarity(self, entity_matcher):
        """Test name similarity with medium similarity names"""
        name1 = "Tech Solutions"
        name2 = "Technical Solutions Group"
        
        similarity = entity_matcher._calculate_name_similarity(name1, name2)
        assert 0.4 < similarity < 0.8
    
    def test_calculate_name_similarity_low_similarity(self, entity_matcher):
        """Test name similarity with low similarity names"""
        name1 = "Tech Solutions"
        name2 = "Accounting Services"
        
        similarity = entity_matcher._calculate_name_similarity(name1, name2)
        assert similarity < 0.4
    
    def test_calculate_name_similarity_empty_names(self, entity_matcher):
        """Test name similarity with empty names"""
        test_cases = [
            ("", ""),
            ("Tech Solutions", ""),
            ("", "Tech Solutions"),
            (None, "Tech Solutions"),
            ("Tech Solutions", None)
        ]
        
        for name1, name2 in test_cases:
            similarity = entity_matcher._calculate_name_similarity(name1, name2)
            assert similarity == 0.0
    
    def test_calculate_name_similarity_case_insensitive(self, entity_matcher):
        """Test that name similarity is case insensitive"""
        name1 = "TECH SOLUTIONS AUSTRALIA"
        name2 = "tech solutions australia"
        
        similarity = entity_matcher._calculate_name_similarity(name1, name2)
        assert similarity == 1.0
    
    def test_calculate_name_similarity_token_based(self, entity_matcher):
        """Test token-based similarity component"""
        # Names with same tokens in different order
        name1 = "Australia Tech Solutions"
        name2 = "Tech Solutions Australia"
        
        similarity = entity_matcher._calculate_name_similarity(name1, name2)
        # Should be high due to token overlap despite different order
        assert similarity > 0.8
    
    def test_calculate_name_similarity_partial_token_overlap(self, entity_matcher):
        """Test names with partial token overlap"""
        name1 = "Tech Solutions Australia"
        name2 = "Tech Consulting Australia"
        
        similarity = entity_matcher._calculate_name_similarity(name1, name2)
        # Should have moderate similarity due to "Tech" and "Australia" overlap
        assert 0.4 < similarity < 0.8
    
    def test_calculate_name_similarity_common_business_suffixes(self, entity_matcher):
        """Test similarity ignores common business suffixes"""
        name1 = "Tech Solutions"
        name2 = "Tech Solutions Pty Ltd"
        
        # Should have high similarity despite Pty Ltd suffix
        similarity = entity_matcher._calculate_name_similarity(name1, name2)
        assert similarity > 0.8


class TestQuickNameSimilarity:
    """Test quick name similarity for initial filtering"""
    
    def test_quick_name_similarity_exact_match(self, entity_matcher):
        """Test quick similarity with exact match"""
        name1 = "Tech Solutions"
        name2 = "Tech Solutions"
        
        similarity = entity_matcher._quick_name_similarity(name1, name2)
        assert similarity == 1.0
    
    def test_quick_name_similarity_sequence_based(self, entity_matcher):
        """Test that quick similarity uses sequence matching"""
        name1 = "Technology Solutions"
        name2 = "Tech Solutions"
        
        similarity = entity_matcher._quick_name_similarity(name1, name2)
        # Should be reasonably high due to character overlap
        assert similarity > 0.6
    
    def test_quick_name_similarity_case_insensitive(self, entity_matcher):
        """Test that quick similarity is case insensitive"""
        name1 = "TECH SOLUTIONS"
        name2 = "tech solutions"
        
        similarity = entity_matcher._quick_name_similarity(name1, name2)
        assert similarity == 1.0
    
    def test_quick_name_similarity_empty_names(self, entity_matcher):
        """Test quick similarity with empty names"""
        assert entity_matcher._quick_name_similarity("", "") == 0.0
        assert entity_matcher._quick_name_similarity("Tech", "") == 0.0
        assert entity_matcher._quick_name_similarity("", "Tech") == 0.0


class TestSemanticSimilarityCalculation:
    """Test semantic similarity using sentence embeddings"""
    
    @pytest.mark.asyncio
    async def test_semantic_similarity_high_similarity(self, entity_matcher):
        """Test semantic similarity with related business descriptions"""
        cc_record = {
            'company_name': 'Tech Solutions',
            'meta_description': 'Software development and IT consulting services',
            'industry': 'Technology'
        }
        
        abr_record = {
            'entity_name': 'Technology Solutions Pty Ltd',
            'trading_names': ['Tech Solutions', 'IT Services']
        }
        
        # Mock the sentence transformer
        with patch.object(entity_matcher.sentence_model, 'encode') as mock_encode:
            # Mock embeddings that would have high cosine similarity
            mock_encode.return_value = np.array([
                [1.0, 0.8, 0.6, 0.4],  # CC record embedding
                [0.9, 0.8, 0.7, 0.5]   # ABR record embedding
            ])
            
            similarity = await entity_matcher._calculate_semantic_similarity(cc_record, abr_record)
            
            # Should have high similarity
            assert similarity > 0.7
    
    @pytest.mark.asyncio
    async def test_semantic_similarity_low_similarity(self, entity_matcher):
        """Test semantic similarity with unrelated business descriptions"""
        cc_record = {
            'company_name': 'Tech Solutions',
            'meta_description': 'Software development and programming',
            'industry': 'Technology'
        }
        
        abr_record = {
            'entity_name': 'Restaurant Services Pty Ltd',
            'trading_names': ['Fine Dining', 'Catering']
        }
        
        # Mock the sentence transformer
        with patch.object(entity_matcher.sentence_model, 'encode') as mock_encode:
            # Mock embeddings that would have low cosine similarity
            mock_encode.return_value = np.array([
                [1.0, 0.0, 0.0, 0.0],  # CC record embedding
                [0.0, 1.0, 0.0, 0.0]   # ABR record embedding
            ])
            
            similarity = await entity_matcher._calculate_semantic_similarity(cc_record, abr_record)
            
            # Should have low similarity
            assert similarity < 0.3
    
    @pytest.mark.asyncio
    async def test_semantic_similarity_empty_text(self, entity_matcher):
        """Test semantic similarity with empty text fields"""
        cc_record = {'company_name': '', 'meta_description': '', 'industry': ''}
        abr_record = {'entity_name': '', 'trading_names': []}
        
        similarity = await entity_matcher._calculate_semantic_similarity(cc_record, abr_record)
        
        # Should return 0 for empty text
        assert similarity == 0.0
    
    @pytest.mark.asyncio
    async def test_semantic_similarity_exception_handling(self, entity_matcher):
        """Test semantic similarity handles exceptions gracefully"""
        cc_record = {
            'company_name': 'Tech Solutions',
            'meta_description': 'Software development',
            'industry': 'Technology'
        }
        
        abr_record = {
            'entity_name': 'Technology Solutions',
            'trading_names': ['Tech Solutions']
        }
        
        # Mock the sentence transformer to raise an exception
        with patch.object(entity_matcher.sentence_model, 'encode') as mock_encode:
            mock_encode.side_effect = Exception("Model error")
            
            similarity = await entity_matcher._calculate_semantic_similarity(cc_record, abr_record)
            
            # Should return 0 on exception
            assert similarity == 0.0


class TestOverallSimilarityCalculation:
    """Test overall similarity calculation combining multiple methods"""
    
    @pytest.mark.asyncio
    async def test_overall_similarity_calculation_weights(self, entity_matcher, sample_cc_record, sample_abr_record):
        """Test that overall similarity uses correct weights"""
        
        # Mock individual similarity methods
        with patch.object(entity_matcher, '_calculate_name_similarity', return_value=0.8) as mock_name:
            with patch.object(entity_matcher, '_calculate_semantic_similarity', new_callable=AsyncMock, return_value=0.7) as mock_semantic:
                with patch.object(entity_matcher, '_calculate_location_similarity', return_value=0.6) as mock_location:
                    with patch.object(entity_matcher, '_calculate_industry_similarity', return_value=0.5) as mock_industry:
                        
                        similarity = await entity_matcher._calculate_similarity(sample_cc_record, sample_abr_record)
                        
                        # Expected weighted average: 0.8*0.5 + 0.7*0.2 + 0.6*0.15 + 0.5*0.15 = 0.715
                        expected = 0.8 * 0.5 + 0.7 * 0.2 + 0.6 * 0.15 + 0.5 * 0.15
                        assert abs(similarity - expected) < 0.01
    
    @pytest.mark.asyncio
    async def test_overall_similarity_with_trading_names(self, entity_matcher, sample_cc_record):
        """Test that overall similarity considers trading names"""
        abr_record_with_trading = {
            'id': 101,
            'entity_name': 'Different Official Name Pty Ltd',
            'trading_names': ['Tech Solutions Australia', 'TSA'],  # Matches CC name exactly
            'business_names': []
        }
        
        # Mock semantic and other similarities to be low
        with patch.object(entity_matcher, '_calculate_semantic_similarity', new_callable=AsyncMock, return_value=0.3):
            with patch.object(entity_matcher, '_calculate_location_similarity', return_value=0.5):
                with patch.object(entity_matcher, '_calculate_industry_similarity', return_value=0.4):
                    
                    similarity = await entity_matcher._calculate_similarity(sample_cc_record, abr_record_with_trading)
                    
                    # Should have high overall similarity due to exact trading name match
                    assert similarity > 0.7
    
    @pytest.mark.asyncio
    async def test_overall_similarity_with_business_names(self, entity_matcher, sample_cc_record):
        """Test that overall similarity considers business names"""
        abr_record_with_business = {
            'id': 101,
            'entity_name': 'Unrelated Official Name Pty Ltd',
            'trading_names': [],
            'business_names': ['Tech Solutions Australia Group']  # Similar to CC name
        }
        
        # Mock other similarities to be neutral
        with patch.object(entity_matcher, '_calculate_semantic_similarity', new_callable=AsyncMock, return_value=0.5):
            with patch.object(entity_matcher, '_calculate_location_similarity', return_value=0.5):
                with patch.object(entity_matcher, '_calculate_industry_similarity', return_value=0.5):
                    
                    similarity = await entity_matcher._calculate_similarity(sample_cc_record, abr_record_with_business)
                    
                    # Should have reasonably high similarity due to business name match
                    assert similarity > 0.6
    
    @pytest.mark.asyncio
    async def test_overall_similarity_maximum_clamping(self, entity_matcher, sample_cc_record, sample_abr_record):
        """Test that overall similarity is clamped to maximum of 1.0"""
        
        # Mock all individual similarities to be very high
        with patch.object(entity_matcher, '_calculate_name_similarity', return_value=1.0):
            with patch.object(entity_matcher, '_calculate_semantic_similarity', new_callable=AsyncMock, return_value=1.0):
                with patch.object(entity_matcher, '_calculate_location_similarity', return_value=1.0):
                    with patch.object(entity_matcher, '_calculate_industry_similarity', return_value=1.0):
                        
                        similarity = await entity_matcher._calculate_similarity(sample_cc_record, sample_abr_record)
                        
                        # Should be clamped to 1.0
                        assert similarity == 1.0


class TestLocationSimilarityCalculation:
    """Test location-based similarity calculation"""
    
    def test_location_similarity_placeholder(self, entity_matcher, sample_cc_record, sample_abr_record):
        """Test location similarity returns neutral score"""
        similarity = entity_matcher._calculate_location_similarity(sample_cc_record, sample_abr_record)
        
        # Currently returns neutral score of 0.5
        assert similarity == 0.5
    
    def test_location_similarity_consistent(self, entity_matcher):
        """Test that location similarity is consistent across calls"""
        cc_record = {'id': 1}
        abr_record = {'id': 2, 'address_state': 'NSW', 'address_suburb': 'Sydney'}
        
        similarity1 = entity_matcher._calculate_location_similarity(cc_record, abr_record)
        similarity2 = entity_matcher._calculate_location_similarity(cc_record, abr_record)
        
        assert similarity1 == similarity2


class TestIndustrySimilarityCalculation:
    """Test industry-based similarity calculation"""
    
    def test_industry_similarity_with_cc_industry(self, entity_matcher):
        """Test industry similarity when CC record has industry"""
        cc_record = {'industry': 'Technology'}
        abr_record = {'entity_name': 'Some Company'}
        
        similarity = entity_matcher._calculate_industry_similarity(cc_record, abr_record)
        
        # Should return neutral score when CC has industry (0.5)
        assert similarity == 0.5
    
    def test_industry_similarity_without_cc_industry(self, entity_matcher):
        """Test industry similarity when CC record has no industry"""
        cc_record = {'industry': ''}
        abr_record = {'entity_name': 'Some Company'}
        
        similarity = entity_matcher._calculate_industry_similarity(cc_record, abr_record)
        
        # Should return 0 when no industry info
        assert similarity == 0.0
    
    def test_industry_similarity_missing_field(self, entity_matcher):
        """Test industry similarity when industry field is missing"""
        cc_record = {}
        abr_record = {'entity_name': 'Some Company'}
        
        similarity = entity_matcher._calculate_industry_similarity(cc_record, abr_record)
        
        # Should return 0 when industry field is missing
        assert similarity == 0.0


class TestSimilarityPerformanceOptimizations:
    """Test performance optimizations in similarity calculations"""
    
    @pytest.mark.asyncio
    async def test_similarity_calculation_efficiency(self, entity_matcher, sample_cc_record):
        """Test that similarity calculation is efficient for large candidate sets"""
        
        # Create multiple ABR records
        abr_records = []
        for i in range(10):
            abr_records.append({
                'id': 100 + i,
                'entity_name': f'Company {i:02d} Pty Ltd',
                'trading_names': [f'Company{i:02d}'],
                'business_names': []
            })
        
        # Mock semantic similarity to avoid actual model calls
        with patch.object(entity_matcher, '_calculate_semantic_similarity', new_callable=AsyncMock, return_value=0.5):
            
            # Calculate similarities for all records
            similarities = []
            for abr_record in abr_records:
                similarity = await entity_matcher._calculate_similarity(sample_cc_record, abr_record)
                similarities.append(similarity)
            
            # All should complete and return valid similarity scores
            assert len(similarities) == 10
            assert all(0.0 <= sim <= 1.0 for sim in similarities)
    
    def test_similarity_calculation_caching_opportunity(self, entity_matcher):
        """Test that repeated similarity calculations could benefit from caching"""
        name1 = "Tech Solutions Australia"
        name2 = "Technology Solutions Australia Pty Ltd"
        
        # Calculate similarity multiple times
        sim1 = entity_matcher._calculate_name_similarity(name1, name2)
        sim2 = entity_matcher._calculate_name_similarity(name1, name2)
        sim3 = entity_matcher._calculate_name_similarity(name1, name2)
        
        # Should return consistent results (caching opportunity)
        assert sim1 == sim2 == sim3


class TestSimilarityEdgeCases:
    """Test edge cases in similarity calculations"""
    
    @pytest.mark.asyncio
    async def test_similarity_with_none_values(self, entity_matcher):
        """Test similarity calculation with None values in records"""
        cc_record = {
            'id': 1,
            'company_name': None,
            'industry': None,
            'meta_description': None
        }
        
        abr_record = {
            'id': 2,
            'entity_name': None,
            'trading_names': None,
            'business_names': None
        }
        
        similarity = await entity_matcher._calculate_similarity(cc_record, abr_record)
        
        # Should handle None values gracefully and return low similarity
        assert 0.0 <= similarity <= 0.3
    
    @pytest.mark.asyncio
    async def test_similarity_with_unicode_characters(self, entity_matcher):
        """Test similarity calculation with Unicode characters"""
        cc_record = {
            'company_name': 'Café Solutions Australia',
            'industry': 'Hospitality',
            'meta_description': 'Café management solutions'
        }
        
        abr_record = {
            'entity_name': 'Cafe Solutions Australia Pty Ltd',
            'trading_names': ['Café Solutions'],
            'business_names': []
        }
        
        # Mock semantic similarity
        with patch.object(entity_matcher, '_calculate_semantic_similarity', new_callable=AsyncMock, return_value=0.8):
            
            similarity = await entity_matcher._calculate_similarity(cc_record, abr_record)
            
            # Should handle Unicode characters and return high similarity
            assert similarity > 0.7
    
    @pytest.mark.asyncio
    async def test_similarity_with_very_long_names(self, entity_matcher):
        """Test similarity calculation with very long company names"""
        long_name = "Very Long Company Name That Goes On And On With Many Words " * 5
        
        cc_record = {
            'company_name': long_name,
            'industry': 'Business Services',
            'meta_description': 'Long business description'
        }
        
        abr_record = {
            'entity_name': long_name + ' Pty Ltd',
            'trading_names': [],
            'business_names': []
        }
        
        similarity = await entity_matcher._calculate_similarity(cc_record, abr_record)
        
        # Should handle long names and return high similarity
        assert similarity > 0.8


if __name__ == '__main__':
    pytest.main([__file__, '-v'])