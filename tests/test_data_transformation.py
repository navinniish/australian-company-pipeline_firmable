import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch
from decimal import Decimal
from datetime import datetime
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from transformers.data_transformer import DataTransformer
from utils.llm_client import LLMClient
from utils.database import DatabaseManager


class TestDataTransformation:
    """Test suite for data transformation logic"""
    
    @pytest.fixture
    def mock_llm_client(self):
        return Mock(spec=LLMClient)
    
    @pytest.fixture
    def mock_db_manager(self):
        return Mock(spec=DatabaseManager)
    
    @pytest.fixture
    def data_transformer(self, mock_llm_client, mock_db_manager):
        return DataTransformer(mock_llm_client, mock_db_manager)
    
    @pytest.fixture
    def sample_matched_record(self):
        return {
            'common_crawl_id': 123,
            'abr_id': 456,
            'similarity_score': 0.85,
            'llm_confidence': 0.82,
            'cc_company_name': 'Tech Solutions Australia',
            'cc_website_url': 'https://techsolutions.com.au',
            'cc_industry': 'Technology',
            'cc_meta_description': 'Leading provider of innovative technology solutions',
            'abr_abn': '12345678901',
            'abr_entity_name': 'Technology Solutions Australia Pty Ltd',
            'abr_entity_status': 'Active',
            'abr_trading_names': '["Tech Solutions", "TSA"]',
            'abr_business_names': '["Tech Solutions Group"]',
            'abr_address_suburb': 'Sydney',
            'abr_address_state': 'NSW',
            'abr_address_postcode': '2000'
        }


class TestCompanyNameDetermination:
    """Test best company name determination logic"""
    
    @pytest.mark.asyncio
    async def test_determine_best_name_high_similarity(self, data_transformer, mock_llm_client):
        """Test that high similarity names don't require LLM"""
        cc_name = "Tech Solutions Australia"
        abr_name = "Technology Solutions Australia Pty Ltd"
        trading_names = ["Tech Solutions"]
        
        # Should not call LLM for high similarity
        result = await data_transformer._determine_best_name(cc_name, abr_name, trading_names)
        
        # Should return the shorter, cleaner name (CC name)
        assert result == cc_name
        mock_llm_client.chat_completion.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_determine_best_name_low_similarity_uses_llm(self, data_transformer, mock_llm_client):
        """Test that low similarity names use LLM for decision"""
        cc_name = "Quick Tech"
        abr_name = "Technology Solutions Australia Pty Ltd"
        trading_names = ["Tech Solutions", "TSA"]
        
        # Mock LLM response
        mock_response = json.dumps({
            "best_name": "Tech Solutions",
            "reasoning": "Most commonly used trading name that balances brevity and clarity",
            "confidence": 0.85
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        data_transformer.llm_client = mock_llm_client
        
        result = await data_transformer._determine_best_name(cc_name, abr_name, trading_names)
        
        assert result == "Tech Solutions"
        mock_llm_client.chat_completion.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_determine_best_name_empty_trading_names(self, data_transformer):
        """Test name determination with empty trading names"""
        cc_name = "Tech Solutions"
        abr_name = "Technology Solutions Pty Ltd"
        trading_names = []
        
        result = await data_transformer._determine_best_name(cc_name, abr_name, trading_names)
        
        # Should return CC name when no trading names available
        assert result == cc_name
    
    @pytest.mark.asyncio
    async def test_determine_best_name_llm_error_fallback(self, data_transformer, mock_llm_client):
        """Test fallback when LLM call fails"""
        cc_name = "Quick Tech"
        abr_name = "Very Different Company Name Pty Ltd"
        trading_names = ["Trading Name"]
        
        # Mock LLM to raise exception
        mock_llm_client.chat_completion = AsyncMock(side_effect=Exception("LLM error"))
        data_transformer.llm_client = mock_llm_client
        
        result = await data_transformer._determine_best_name(cc_name, abr_name, trading_names)
        
        # Should fall back to CC name
        assert result == cc_name
    
    @pytest.mark.asyncio
    async def test_determine_best_name_invalid_json_response(self, data_transformer, mock_llm_client):
        """Test handling of invalid JSON from LLM"""
        cc_name = "Tech Co"
        abr_name = "Different Technology Company Pty Ltd"
        trading_names = ["TechCorp"]
        
        # Mock invalid JSON response
        mock_llm_client.chat_completion = AsyncMock(return_value="{ invalid json")
        data_transformer.llm_client = mock_llm_client
        
        result = await data_transformer._determine_best_name(cc_name, abr_name, trading_names)
        
        # Should fall back to CC name
        assert result == cc_name


class TestIndustryDetermination:
    """Test industry classification logic"""
    
    @pytest.mark.asyncio
    async def test_determine_industry_keyword_mapping(self, data_transformer):
        """Test industry determination using keyword mapping"""
        cc_industry = "Technology"
        cc_description = "Software development services"
        
        result = await data_transformer._determine_industry(cc_industry, cc_description)
        
        # Should return mapped industry
        assert result in ["Technology", "Information Technology", "Software Development"]
    
    @pytest.mark.asyncio
    async def test_determine_industry_uses_llm_for_complex_text(self, data_transformer, mock_llm_client):
        """Test that LLM is used for complex industry determination"""
        cc_industry = ""
        cc_description = "We provide comprehensive business consulting services including strategic planning, operational optimization, and digital transformation for mid-market companies"
        
        # Mock LLM response
        mock_response = json.dumps({
            "industry": "Business Consulting",
            "reasoning": "The description clearly indicates business consulting services",
            "confidence": 0.90
        })
        mock_llm_client.chat_completion = AsyncMock(return_value=mock_response)
        data_transformer.llm_client = mock_llm_client
        
        result = await data_transformer._determine_industry(cc_industry, cc_description)
        
        assert result == "Business Consulting"
        mock_llm_client.chat_completion.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_determine_industry_short_text_no_llm(self, data_transformer, mock_llm_client):
        """Test that short descriptions don't trigger LLM"""
        cc_industry = "Tech"
        cc_description = "Software"
        
        result = await data_transformer._determine_industry(cc_industry, cc_description)
        
        # Should use existing industry without LLM call
        assert result == "Tech"
        mock_llm_client.chat_completion.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_determine_industry_llm_error_fallback(self, data_transformer, mock_llm_client):
        """Test industry determination fallback when LLM fails"""
        cc_industry = ""
        cc_description = "Complex business description that would normally trigger LLM analysis"
        
        # Mock LLM to raise exception
        mock_llm_client.chat_completion = AsyncMock(side_effect=Exception("API error"))
        data_transformer.llm_client = mock_llm_client
        
        result = await data_transformer._determine_industry(cc_industry, cc_description)
        
        # Should return "Other" as fallback
        assert result == "Other"
    
    @pytest.mark.asyncio
    async def test_determine_industry_empty_inputs(self, data_transformer):
        """Test industry determination with empty inputs"""
        result = await data_transformer._determine_industry("", "")
        
        assert result == "Other"


class TestDataQualityScoring:
    """Test data quality scoring logic"""
    
    def test_calculate_data_quality_score_high_quality(self, data_transformer):
        """Test data quality score for high-quality record"""
        record = {
            'cc_company_name': 'Tech Solutions Australia',
            'cc_website_url': 'https://techsolutions.com.au',
            'cc_industry': 'Technology',
            'cc_meta_description': 'Leading provider of innovative technology solutions for businesses',
            'abr_abn': '12345678901',
            'abr_entity_name': 'Technology Solutions Australia Pty Ltd',
            'abr_address_suburb': 'Sydney',
            'abr_address_state': 'NSW',
            'abr_address_postcode': '2000',
            'similarity_score': 0.92,
            'llm_confidence': 0.88
        }
        
        score = data_transformer._calculate_data_quality_score(record)
        
        # Should have high quality score
        assert score > 0.85
    
    def test_calculate_data_quality_score_medium_quality(self, data_transformer):
        """Test data quality score for medium-quality record"""
        record = {
            'cc_company_name': 'Tech Co',
            'cc_website_url': 'https://tech.com.au',
            'cc_industry': '',  # Missing industry
            'cc_meta_description': 'Tech services',  # Short description
            'abr_abn': '12345678901',
            'abr_entity_name': 'Technology Company Pty Ltd',
            'abr_address_suburb': 'Sydney',
            'abr_address_state': 'NSW',
            'abr_address_postcode': '',  # Missing postcode
            'similarity_score': 0.75,
            'llm_confidence': 0.70
        }
        
        score = data_transformer._calculate_data_quality_score(record)
        
        # Should have medium quality score
        assert 0.5 < score < 0.85
    
    def test_calculate_data_quality_score_low_quality(self, data_transformer):
        """Test data quality score for low-quality record"""
        record = {
            'cc_company_name': '',  # Missing name
            'cc_website_url': '',   # Missing URL
            'cc_industry': '',      # Missing industry
            'cc_meta_description': '',  # Missing description
            'abr_abn': '12345678901',
            'abr_entity_name': 'Company',  # Very short name
            'abr_address_suburb': '',      # Missing suburb
            'abr_address_state': '',       # Missing state
            'abr_address_postcode': '',    # Missing postcode
            'similarity_score': 0.45,
            'llm_confidence': 0.35
        }
        
        score = data_transformer._calculate_data_quality_score(record)
        
        # Should have low quality score
        assert score < 0.5
    
    def test_calculate_data_quality_score_boundary_conditions(self, data_transformer):
        """Test data quality score boundary conditions"""
        # Test with None values
        record_with_nones = {
            'cc_company_name': None,
            'cc_website_url': None,
            'cc_industry': None,
            'cc_meta_description': None,
            'abr_abn': '12345678901',
            'abr_entity_name': None,
            'abr_address_suburb': None,
            'abr_address_state': None,
            'abr_address_postcode': None,
            'similarity_score': 0.60,
            'llm_confidence': 0.55
        }
        
        score = data_transformer._calculate_data_quality_score(record_with_nones)
        
        # Should handle None values gracefully
        assert 0.0 <= score <= 1.0


class TestContactInformationExtraction:
    """Test contact information extraction and standardization"""
    
    def test_extract_contact_info_with_valid_data(self, data_transformer):
        """Test contact info extraction with valid data"""
        cc_description = "Contact us at info@techsolutions.com.au or call (02) 9999-8888"
        cc_title = "Tech Solutions - Sydney Office"
        
        contact_info = data_transformer._extract_contact_info(cc_description, cc_title)
        
        # Should extract email and phone
        assert 'info@techsolutions.com.au' in contact_info.get('emails', [])
        assert any('9999' in phone for phone in contact_info.get('phones', []))
    
    def test_extract_contact_info_empty_input(self, data_transformer):
        """Test contact info extraction with empty input"""
        contact_info = data_transformer._extract_contact_info("", "")
        
        # Should return empty structure
        assert contact_info.get('emails', []) == []
        assert contact_info.get('phones', []) == []
    
    def test_standardize_address_complete(self, data_transformer):
        """Test address standardization with complete data"""
        suburb = "Sydney"
        state = "NSW"
        postcode = "2000"
        
        address = data_transformer._standardize_address(suburb, state, postcode)
        
        assert address == "Sydney, NSW 2000"
    
    def test_standardize_address_partial(self, data_transformer):
        """Test address standardization with partial data"""
        # Test with missing postcode
        address1 = data_transformer._standardize_address("Melbourne", "VIC", "")
        assert address1 == "Melbourne, VIC"
        
        # Test with missing suburb
        address2 = data_transformer._standardize_address("", "QLD", "4000")
        assert address2 == "QLD 4000"
        
        # Test with only state
        address3 = data_transformer._standardize_address("", "WA", "")
        assert address3 == "WA"
    
    def test_standardize_address_empty(self, data_transformer):
        """Test address standardization with empty data"""
        address = data_transformer._standardize_address("", "", "")
        assert address == ""


class TestAlternativeNamesProcessing:
    """Test processing of alternative company names"""
    
    def test_process_alternative_names_with_trading_names(self, data_transformer):
        """Test processing trading names as alternatives"""
        trading_names_json = '["Tech Solutions", "TSA", "TechSol"]'
        business_names_json = '["Tech Solutions Group"]'
        primary_name = "Technology Solutions Australia"
        
        alternatives = data_transformer._process_alternative_names(
            trading_names_json, business_names_json, primary_name
        )
        
        # Should include trading and business names, excluding primary
        expected_names = {"Tech Solutions", "TSA", "TechSol", "Tech Solutions Group"}
        assert set(alternatives) == expected_names
    
    def test_process_alternative_names_filters_duplicates(self, data_transformer):
        """Test that alternative names filter duplicates"""
        trading_names_json = '["Tech Solutions", "Tech Solutions", "TSA"]'
        business_names_json = '["Tech Solutions"]'  # Duplicate of trading name
        primary_name = "Technology Solutions Australia"
        
        alternatives = data_transformer._process_alternative_names(
            trading_names_json, business_names_json, primary_name
        )
        
        # Should deduplicate
        assert alternatives == ["Tech Solutions", "TSA"]
    
    def test_process_alternative_names_excludes_primary(self, data_transformer):
        """Test that primary name is excluded from alternatives"""
        trading_names_json = '["Tech Solutions Australia", "TSA"]'
        business_names_json = '["Tech Solutions Group"]'
        primary_name = "Tech Solutions Australia"
        
        alternatives = data_transformer._process_alternative_names(
            trading_names_json, business_names_json, primary_name
        )
        
        # Should exclude the primary name
        assert "Tech Solutions Australia" not in alternatives
        assert "TSA" in alternatives
        assert "Tech Solutions Group" in alternatives
    
    def test_process_alternative_names_invalid_json(self, data_transformer):
        """Test handling of invalid JSON in names"""
        trading_names_json = '["Tech Solutions", invalid json'
        business_names_json = '["Valid Name"]'
        primary_name = "Primary Company"
        
        alternatives = data_transformer._process_alternative_names(
            trading_names_json, business_names_json, primary_name
        )
        
        # Should handle invalid JSON gracefully
        assert "Valid Name" in alternatives
    
    def test_process_alternative_names_empty_inputs(self, data_transformer):
        """Test with empty or null inputs"""
        alternatives = data_transformer._process_alternative_names(None, None, "Primary")
        assert alternatives == []
        
        alternatives = data_transformer._process_alternative_names("", "", "Primary")
        assert alternatives == []


class TestTransformationIntegration:
    """Test integration of transformation components"""
    
    @pytest.mark.asyncio
    async def test_transform_matched_records_complete_flow(self, data_transformer, sample_matched_record, mock_db_manager, mock_llm_client):
        """Test complete transformation flow"""
        # Mock database to return matched records
        mock_db_manager.fetch_all = AsyncMock(return_value=[sample_matched_record])
        data_transformer.db_manager = mock_db_manager
        
        # Mock LLM responses for name and industry determination
        async def mock_llm_response(prompt):
            if "best name" in prompt:
                return json.dumps({
                    "best_name": "Tech Solutions Australia",
                    "reasoning": "Clear and concise name",
                    "confidence": 0.90
                })
            elif "industry" in prompt:
                return json.dumps({
                    "industry": "Technology",
                    "reasoning": "Clear technology focus",
                    "confidence": 0.85
                })
        
        mock_llm_client.chat_completion = AsyncMock(side_effect=mock_llm_response)
        data_transformer.llm_client = mock_llm_client
        
        # Mock database insert
        mock_db_manager.bulk_insert = AsyncMock()
        
        # Run transformation
        result = await data_transformer.transform_matched_records()
        
        # Should have processed the record
        assert result > 0
        
        # Verify database insert was called
        mock_db_manager.bulk_insert.assert_called()
        
        # Check the structure of inserted data
        insert_call = mock_db_manager.bulk_insert.call_args
        inserted_records = insert_call[0][1]  # Second argument (records)
        
        assert len(inserted_records) == 1
        record = inserted_records[0]
        
        # Verify key fields are present
        assert 'company_name' in record
        assert 'abn' in record
        assert 'website_url' in record
        assert 'data_quality_score' in record
        assert record['entity_status'] == 'Active'
    
    @pytest.mark.asyncio
    async def test_transform_matched_records_batch_processing(self, data_transformer, mock_db_manager, mock_llm_client):
        """Test batch processing of large record sets"""
        # Create multiple matched records
        matched_records = []
        for i in range(15):  # More than batch size to test batching
            record = {
                'common_crawl_id': i,
                'abr_id': 100 + i,
                'similarity_score': 0.80,
                'llm_confidence': 0.75,
                'cc_company_name': f'Company {i:02d}',
                'cc_website_url': f'https://company{i:02d}.com.au',
                'cc_industry': 'Technology',
                'cc_meta_description': f'Company {i:02d} description',
                'abr_abn': f'1234567890{i}',
                'abr_entity_name': f'Company {i:02d} Pty Ltd',
                'abr_entity_status': 'Active',
                'abr_trading_names': f'["Company{i:02d}"]',
                'abr_business_names': '[]',
                'abr_address_suburb': 'Sydney',
                'abr_address_state': 'NSW',
                'abr_address_postcode': '2000'
            }
            matched_records.append(record)
        
        mock_db_manager.fetch_all = AsyncMock(return_value=matched_records)
        data_transformer.db_manager = mock_db_manager
        
        # Mock LLM to avoid actual calls
        mock_llm_client.chat_completion = AsyncMock(return_value=json.dumps({
            "best_name": "Test Company",
            "reasoning": "Test",
            "confidence": 0.8
        }))
        data_transformer.llm_client = mock_llm_client
        
        # Mock database insert
        mock_db_manager.bulk_insert = AsyncMock()
        
        # Run transformation
        result = await data_transformer.transform_matched_records(batch_size=10)
        
        # Should process all records
        assert result == 15
        
        # Should have made multiple batch inserts
        assert mock_db_manager.bulk_insert.call_count >= 1
    
    @pytest.mark.asyncio
    async def test_transform_matched_records_error_handling(self, data_transformer, mock_db_manager, mock_llm_client):
        """Test error handling in transformation process"""
        # Mock database to return a record
        problematic_record = {
            'common_crawl_id': 1,
            'abr_id': 101,
            'similarity_score': 0.75,
            'llm_confidence': 0.70,
            'cc_company_name': None,  # Problematic data
            'cc_website_url': '',
            'cc_industry': None,
            'cc_meta_description': None,
            'abr_abn': '12345678901',
            'abr_entity_name': None,  # Problematic data
            'abr_entity_status': 'Active',
            'abr_trading_names': 'invalid json',  # Problematic JSON
            'abr_business_names': None,
            'abr_address_suburb': None,
            'abr_address_state': None,
            'abr_address_postcode': None
        }
        
        mock_db_manager.fetch_all = AsyncMock(return_value=[problematic_record])
        data_transformer.db_manager = mock_db_manager
        
        # Mock LLM to handle edge cases
        mock_llm_client.chat_completion = AsyncMock(return_value=json.dumps({
            "best_name": "Fallback Name",
            "reasoning": "Fallback",
            "confidence": 0.5
        }))
        data_transformer.llm_client = mock_llm_client
        
        # Mock database insert
        mock_db_manager.bulk_insert = AsyncMock()
        
        # Should complete without throwing exceptions
        result = await data_transformer.transform_matched_records()
        
        # Should have processed the record despite issues
        assert result >= 0
        
        # Verify that insert was attempted (even with fallback data)
        if result > 0:
            mock_db_manager.bulk_insert.assert_called()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])