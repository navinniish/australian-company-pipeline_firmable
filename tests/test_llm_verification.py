import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from entity_matching.llm_entity_matcher import LLMEntityMatcher, EntityMatch
from utils.llm_client import LLMClient
from utils.database import DatabaseManager


class TestLLMVerification:
    """Test suite for LLM verification logic in entity matching"""
    
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
            'id': 123,
            'website_url': 'https://techsolutions.com.au',
            'company_name': 'Tech Solutions Australia',
            'industry': 'Technology',
            'meta_description': 'Leading provider of innovative technology solutions',
            'title': 'Tech Solutions - Innovation Partners'
        }
    
    @pytest.fixture
    def sample_abr_record(self):
        return {
            'id': 456,
            'abn': '12345678901',
            'entity_name': 'Technology Solutions Australia Pty Ltd',
            'trading_names': ['Tech Solutions', 'TSA'],
            'business_names': ['Tech Solutions Group'],
            'address_suburb': 'Sydney',
            'address_state': 'NSW',
            'address_postcode': '2000',
            'entity_status': 'Active'
        }


class TestLLMPromptConstruction:
    """Test LLM prompt construction and formatting"""
    
    @pytest.mark.asyncio
    async def test_llm_prompt_includes_all_fields(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test that LLM prompt includes all relevant fields from both records"""
        # Mock LLM response
        mock_response = json.dumps({
            "is_match": True,
            "confidence": 0.85,
            "reasoning": "Strong name similarity and domain alignment",
            "key_factors": ["name_similarity", "domain_match"]
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        similarity_score = 0.75
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, similarity_score)
        
        # Check that LLM was called
        mock_llm_client.chat_completion.assert_called_once()
        
        # Extract the prompt that was passed to LLM
        call_args = mock_llm_client.chat_completion.call_args[0][0]
        
        # Verify prompt contains CC record fields
        assert 'techsolutions.com.au' in call_args
        assert 'Tech Solutions Australia' in call_args
        assert 'Technology' in call_args
        assert 'Leading provider of innovative technology solutions' in call_args
        
        # Verify prompt contains ABR record fields
        assert '12345678901' in call_args
        assert 'Technology Solutions Australia Pty Ltd' in call_args
        assert 'Tech Solutions, TSA' in call_args
        assert 'Sydney, NSW 2000' in call_args
        
        # Verify similarity score is included
        assert '0.750' in call_args
    
    @pytest.mark.asyncio
    async def test_llm_prompt_handles_missing_fields(self, entity_matcher, mock_llm_client):
        """Test that LLM prompt gracefully handles missing fields"""
        cc_record_minimal = {
            'id': 123,
            'website_url': 'https://example.com.au',
            'company_name': 'Example Company'
            # Missing industry, meta_description, title
        }
        
        abr_record_minimal = {
            'id': 456,
            'abn': '98765432109',
            'entity_name': 'Example Business Ltd',
            'entity_status': 'Active'
            # Missing trading_names, business_names, address fields
        }
        
        mock_response = json.dumps({
            "is_match": False,
            "confidence": 0.30,
            "reasoning": "Insufficient information for confident matching"
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(cc_record_minimal, abr_record_minimal, 0.65)
        
        # Should handle missing fields gracefully
        call_args = mock_llm_client.chat_completion.call_args[0][0]
        assert 'N/A' in call_args  # Missing fields should show as N/A
        assert result['is_match'] is False


class TestLLMResponseParsing:
    """Test parsing and validation of LLM responses"""
    
    @pytest.mark.asyncio
    async def test_valid_llm_response_parsing(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test parsing of valid LLM response"""
        mock_response = json.dumps({
            "is_match": True,
            "confidence": 0.92,
            "reasoning": "Strong match - company names are highly similar, domain aligns with business name, and industry is consistent",
            "key_factors": ["name_similarity", "domain_alignment", "industry_consistency"]
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.80)
        
        assert result['is_match'] is True
        assert result['confidence'] == 0.92
        assert 'Strong match' in result['reasoning']
        assert 'key_factors' in result
    
    @pytest.mark.asyncio
    async def test_confidence_boundary_validation(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test that confidence scores are validated to be within 0.0-1.0 range"""
        # Test confidence > 1.0
        mock_response_high = json.dumps({
            "is_match": True,
            "confidence": 1.5,  # Invalid: > 1.0
            "reasoning": "Perfect match"
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response_high)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.80)
        
        # Should clamp to 1.0
        assert result['confidence'] == 1.0
        
        # Test confidence < 0.0
        mock_response_low = json.dumps({
            "is_match": False,
            "confidence": -0.2,  # Invalid: < 0.0
            "reasoning": "No match"
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response_low)
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.80)
        
        # Should clamp to 0.0
        assert result['confidence'] == 0.0
    
    @pytest.mark.asyncio
    async def test_malformed_json_response_handling(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test handling of malformed JSON responses from LLM"""
        # Test malformed JSON
        mock_response = "{ invalid json response"
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.75)
        
        # Should return safe fallback response
        assert result['is_match'] is False
        assert result['confidence'] == 0.0
        assert 'LLM verification failed' in result['reasoning']
    
    @pytest.mark.asyncio
    async def test_missing_required_fields_response(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test handling of LLM responses missing required fields"""
        # Test missing 'is_match' field
        mock_response = json.dumps({
            "confidence": 0.80,
            "reasoning": "Missing is_match field"
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.75)
        
        # Should return safe fallback
        assert result['is_match'] is False
        assert result['confidence'] == 0.0
        assert 'LLM verification failed' in result['reasoning']


class TestLLMVerificationDecisionLogic:
    """Test LLM verification decision making logic"""
    
    @pytest.mark.asyncio
    async def test_high_confidence_match_acceptance(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test that high-confidence matches are accepted"""
        mock_response = json.dumps({
            "is_match": True,
            "confidence": 0.95,
            "reasoning": "Extremely strong match with multiple confirming factors",
            "key_factors": ["exact_name_match", "domain_perfect_alignment", "location_match"]
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.85)
        
        assert result['is_match'] is True
        assert result['confidence'] == 0.95
    
    @pytest.mark.asyncio
    async def test_low_confidence_match_rejection(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test that low-confidence responses result in rejection"""
        mock_response = json.dumps({
            "is_match": False,
            "confidence": 0.25,
            "reasoning": "Names are somewhat similar but significant differences in industry and location suggest different entities",
            "key_factors": ["name_partial_similarity", "industry_mismatch", "location_different"]
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.60)
        
        assert result['is_match'] is False
        assert result['confidence'] == 0.25
    
    @pytest.mark.asyncio
    async def test_borderline_confidence_handling(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test handling of borderline confidence scores"""
        mock_response = json.dumps({
            "is_match": True,
            "confidence": 0.72,  # Between manual review (0.4) and high confidence (0.85)
            "reasoning": "Reasonable match but some uncertainty remains due to slight name variations",
            "key_factors": ["name_similarity", "domain_match", "minor_variations"]
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.70)
        
        assert result['is_match'] is True
        assert result['confidence'] == 0.72
        assert 'uncertainty' in result['reasoning'].lower()


class TestLLMIntegrationWithEntityMatching:
    """Test integration of LLM verification with entity matching process"""
    
    @pytest.mark.asyncio
    async def test_entity_match_creation_from_llm_response(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test creation of EntityMatch objects from LLM verification"""
        mock_response = json.dumps({
            "is_match": True,
            "confidence": 0.88,
            "reasoning": "Strong match with high confidence",
            "key_factors": ["name_match", "domain_alignment"]
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        # Mock the similarity calculation and filtering
        with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
            mock_filter.return_value = [sample_abr_record]
            
            with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
                mock_calc.return_value = 0.75  # Above LLM threshold
                
                matches = await entity_matcher._find_best_matches(sample_cc_record, [sample_abr_record])
                
                assert len(matches) == 1
                match = matches[0]
                
                assert isinstance(match, EntityMatch)
                assert match.common_crawl_id == 123
                assert match.abr_id == 456
                assert match.similarity_score == 0.75
                assert match.matching_method == 'hybrid_llm'
                assert match.llm_confidence == 0.88
                assert match.llm_reasoning == "Strong match with high confidence"
                
                # Manual review should not be required for high confidence (0.88 >= 0.85)
                assert match.manual_review_required is False
    
    @pytest.mark.asyncio
    async def test_manual_review_flag_for_medium_confidence(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test that medium confidence matches are flagged for manual review"""
        mock_response = json.dumps({
            "is_match": True,
            "confidence": 0.75,  # Between 0.60 and 0.85
            "reasoning": "Good match but some uncertainty",
            "key_factors": ["name_similarity"]
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
            mock_filter.return_value = [sample_abr_record]
            
            with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
                mock_calc.return_value = 0.70
                
                matches = await entity_matcher._find_best_matches(sample_cc_record, [sample_abr_record])
                
                assert len(matches) == 1
                match = matches[0]
                
                # Should require manual review for medium confidence
                assert match.manual_review_required is True
                assert match.llm_confidence == 0.75


class TestLLMErrorHandling:
    """Test error handling in LLM verification"""
    
    @pytest.mark.asyncio
    async def test_llm_api_error_handling(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test handling of LLM API errors"""
        # Mock LLM client to raise an exception
        mock_llm_client.chat_completion = AsyncMock(side_effect=Exception("API connection failed"))
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.75)
        
        # Should return safe fallback response
        assert result['is_match'] is False
        assert result['confidence'] == 0.0
        assert 'LLM verification failed' in result['reasoning']
        assert 'API connection failed' in result['reasoning']
    
    @pytest.mark.asyncio
    async def test_llm_timeout_handling(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test handling of LLM request timeouts"""
        # Mock LLM client to raise timeout exception
        mock_llm_client.chat_completion = AsyncMock(side_effect=asyncio.TimeoutError("Request timed out"))
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.80)
        
        # Should return safe fallback response
        assert result['is_match'] is False
        assert result['confidence'] == 0.0
        assert 'LLM verification failed' in result['reasoning']
    
    @pytest.mark.asyncio
    async def test_invalid_json_number_types(self, entity_matcher, mock_llm_client, sample_cc_record, sample_abr_record):
        """Test handling of invalid number types in LLM response"""
        mock_response = json.dumps({
            "is_match": True,
            "confidence": "high",  # Invalid: string instead of number
            "reasoning": "Good match"
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        entity_matcher.llm_client = mock_llm_client
        
        result = await entity_matcher._llm_verify_match(sample_cc_record, sample_abr_record, 0.75)
        
        # Should handle invalid number gracefully
        assert result['is_match'] is False
        assert result['confidence'] == 0.0


class TestLLMPerformanceOptimizations:
    """Test performance optimizations in LLM usage"""
    
    @pytest.mark.asyncio
    async def test_llm_not_called_below_threshold(self, entity_matcher, sample_cc_record, sample_abr_record):
        """Test that LLM is not called for scores below threshold"""
        # Mock similarity calculation to return score below LLM threshold
        with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
            mock_filter.return_value = [sample_abr_record]
            
            with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
                mock_calc.return_value = 0.55  # Below llm_review_threshold (0.60)
                
                # Mock LLM to track if it's called
                with patch.object(entity_matcher, '_llm_verify_match', new_callable=AsyncMock) as mock_llm:
                    matches = await entity_matcher._find_best_matches(sample_cc_record, [sample_abr_record])
                    
                    # LLM should not be called
                    mock_llm.assert_not_called()
                    
                    # Should return no matches due to low similarity
                    assert len(matches) == 0
    
    @pytest.mark.asyncio
    async def test_early_termination_prevents_additional_llm_calls(self, entity_matcher, sample_cc_record, mock_llm_client):
        """Test that early termination prevents unnecessary LLM calls"""
        # Create multiple candidate records
        abr_records = []
        for i in range(3):
            abr_records.append({
                'id': 500 + i,
                'entity_name': f'Match Candidate {i}',
                'entity_status': 'Active',
                'trading_names': [],
                'business_names': []
            })
        
        # Mock first LLM call to return a match
        call_count = 0
        async def mock_llm_response(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {
                    'is_match': True,
                    'confidence': 0.90,
                    'reasoning': 'First candidate is a strong match'
                }
            else:
                return {
                    'is_match': False,
                    'confidence': 0.70,
                    'reasoning': 'Subsequent candidate'
                }
        
        mock_llm_client.chat_completion = AsyncMock(side_effect=mock_llm_response)
        entity_matcher.llm_client = mock_llm_client
        
        with patch.object(entity_matcher, '_filter_candidates') as mock_filter:
            mock_filter.return_value = abr_records
            
            with patch.object(entity_matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
                mock_calc.return_value = 0.75  # Above LLM threshold
                
                matches = await entity_matcher._find_best_matches(sample_cc_record, abr_records)
                
                # Should terminate after first match and return only one match
                assert len(matches) == 1
                assert matches[0].abr_id == 500  # First candidate
                
                # Should only call LLM once due to early termination
                assert call_count == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])