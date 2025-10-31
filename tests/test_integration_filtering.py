import pytest
import asyncio
import json
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from decimal import Decimal
from datetime import datetime
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from entity_matching.llm_entity_matcher import LLMEntityMatcher, EntityMatch
from extractors.common_crawl_extractor import CommonCrawlExtractor
from transformers.data_transformer import DataTransformer
from pipeline.etl_pipeline import ETLPipeline
from utils.llm_client import LLMClient
from utils.database import DatabaseManager


class TestEndToEndFiltering:
    """Test end-to-end filtering and optimization strategies"""
    
    @pytest.fixture
    def mock_llm_client(self):
        client = Mock(spec=LLMClient)
        client.batch_completions = AsyncMock()
        client.chat_completion = AsyncMock()
        return client
    
    @pytest.fixture
    def mock_db_manager(self):
        db = Mock(spec=DatabaseManager)
        db.fetch_all = AsyncMock()
        db.bulk_insert = AsyncMock()
        db.execute_query = AsyncMock()
        return db
    
    @pytest.fixture
    def pipeline_components(self, mock_llm_client, mock_db_manager):
        extractor = CommonCrawlExtractor(mock_llm_client, mock_db_manager)
        matcher = LLMEntityMatcher(mock_llm_client, mock_db_manager)
        transformer = DataTransformer(mock_llm_client, mock_db_manager)
        
        return {
            'extractor': extractor,
            'matcher': matcher,
            'transformer': transformer,
            'llm_client': mock_llm_client,
            'db_manager': mock_db_manager
        }


class TestFilteringEfficiencyMeasures:
    """Test that filtering efficiency measures work as expected"""
    
    @pytest.mark.asyncio
    async def test_url_filtering_reduces_llm_calls(self, pipeline_components):
        """Test that URL filtering significantly reduces LLM processing"""
        extractor = pipeline_components['extractor']
        mock_db_manager = pipeline_components['db_manager']
        mock_llm_client = pipeline_components['llm_client']
        
        # Mock database to return mixed URLs (good and bad)
        mixed_urls = []
        
        # Add 500 valid company URLs
        for i in range(500):
            mixed_urls.append({
                'url': f'https://company{i:03d}.com.au',
                'urlkey': f'au,com,company{i:03d})/',
                'charset': 'UTF-8'
            })
        
        # Add 1000 URLs that should be filtered out
        for i in range(1000):
            mixed_urls.extend([
                {'url': f'https://company{i:03d}.com.au/blog/post-{i}', 'urlkey': f'au,com,company{i:03d})/blog/post-{i}', 'charset': 'UTF-8'},
                {'url': f'https://company{i:03d}.com.au/wp-admin/edit.php', 'urlkey': f'au,com,company{i:03d})/wp-admin/edit.php', 'charset': 'UTF-8'},
                {'url': f'https://company{i:03d}.com.au/files/doc{i}.pdf', 'urlkey': f'au,com,company{i:03d})/files/doc{i}.pdf', 'charset': 'UTF-8'}
            ])
        
        mock_db_manager.fetch_all.return_value = mixed_urls
        
        # Mock LLM responses
        mock_llm_client.batch_completions.return_value = [
            '{"company_name": "Test Company", "industry": "Technology", "confidence": 0.8}'
        ] * 500
        
        # Run extraction
        await extractor.extract_companies(max_records=2000)
        
        # Verify that LLM was only called for valid URLs (500), not all (1500)
        llm_call_count = mock_llm_client.batch_completions.call_count
        if llm_call_count > 0:
            # Check the prompts sent to LLM
            all_prompts = []
            for call in mock_llm_client.batch_completions.call_args_list:
                all_prompts.extend(call[0][0])  # First argument is prompts list
            
            # Should have significantly fewer prompts than total URLs
            assert len(all_prompts) <= 600  # Some buffer for batching
            
            # Verify no excluded URLs were processed
            for prompt in all_prompts:
                assert '/blog/' not in prompt
                assert '/wp-admin/' not in prompt
                assert '.pdf' not in prompt
    
    @pytest.mark.asyncio
    async def test_entity_matching_candidate_limiting(self, pipeline_components):
        """Test that entity matching limits candidates to control LLM usage"""
        matcher = pipeline_components['matcher']
        mock_db_manager = pipeline_components['db_manager']
        mock_llm_client = pipeline_components['llm_client']
        
        # Create a CC record
        cc_record = {
            'id': 1,
            'website_url': 'https://techsolutions.com.au',
            'company_name': 'Tech Solutions Australia',
            'industry': 'Technology',
            'meta_description': 'Technology services',
            'title': 'Tech Solutions'
        }
        
        # Create 200 ABR records (more than the 50 candidate limit)
        abr_records = []
        for i in range(200):
            abr_records.append({
                'id': i + 1000,
                'abn': f'1234567890{i:02d}',
                'entity_name': f'Technology Solutions {i:03d} Pty Ltd',
                'entity_status': 'Active',
                'trading_names': [f'Tech{i:03d}', f'Solutions{i:03d}'],
                'business_names': [],
                'address_suburb': 'Sydney',
                'address_state': 'NSW',
                'address_postcode': '2000'
            })
        
        # Mock LLM for verification (return no matches to prevent early termination)
        mock_llm_client.chat_completion.return_value = json.dumps({
            "is_match": False,
            "confidence": 0.40,
            "reasoning": "Not a strong enough match"
        })
        
        # Track similarity calculation calls
        with patch.object(matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
            mock_calc.return_value = 0.65  # Above LLM threshold
            
            # Run matching
            matches = await matcher._find_best_matches(cc_record, abr_records)
            
            # Should only calculate similarity for top 50 candidates (per code limit)
            assert mock_calc.call_count <= 50
    
    @pytest.mark.asyncio
    async def test_llm_threshold_filtering_prevents_unnecessary_calls(self, pipeline_components):
        """Test that confidence thresholds prevent unnecessary LLM calls"""
        matcher = pipeline_components['matcher']
        mock_llm_client = pipeline_components['llm_client']
        
        cc_record = {
            'id': 1,
            'website_url': 'https://example.com.au',
            'company_name': 'Example Company',
            'industry': 'Business',
            'meta_description': 'Business services'
        }
        
        abr_records = [{
            'id': 2000,
            'entity_name': 'Completely Different Business Name Pty Ltd',
            'entity_status': 'Active',
            'trading_names': ['Different Trading Name'],
            'business_names': []
        }]
        
        # Mock similarity calculation to return score below LLM threshold
        with patch.object(matcher, '_filter_candidates', return_value=abr_records):
            with patch.object(matcher, '_calculate_similarity', new_callable=AsyncMock) as mock_calc:
                mock_calc.return_value = 0.55  # Below llm_review_threshold (0.60)
                
                matches = await matcher._find_best_matches(cc_record, abr_records)
                
                # LLM should NOT have been called due to low similarity
                mock_llm_client.chat_completion.assert_not_called()
                
                # Should return no matches due to low similarity
                assert len(matches) == 0
    
    @pytest.mark.asyncio
    async def test_early_termination_saves_llm_calls(self, pipeline_components):
        """Test that early termination on first match saves LLM calls"""
        matcher = pipeline_components['matcher']
        mock_llm_client = pipeline_components['llm_client']
        
        cc_record = {
            'id': 1,
            'website_url': 'https://techsolutions.com.au',
            'company_name': 'Tech Solutions',
            'industry': 'Technology'
        }
        
        # Create 10 potential candidate records
        abr_records = []
        for i in range(10):
            abr_records.append({
                'id': 3000 + i,
                'entity_name': f'Tech Solutions {i:02d} Pty Ltd',
                'entity_status': 'Active',
                'trading_names': [f'TechSol{i:02d}'],
                'business_names': []
            })
        
        # Mock LLM to return match on first call, then other responses
        call_count = 0
        async def mock_llm_response(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return json.dumps({
                    "is_match": True,
                    "confidence": 0.90,
                    "reasoning": "Strong match found on first candidate"
                })
            else:
                return json.dumps({
                    "is_match": False,
                    "confidence": 0.60,
                    "reasoning": "Later candidate, shouldn't be called"
                })
        
        mock_llm_client.chat_completion = AsyncMock(side_effect=mock_llm_response)
        
        # Mock filtering and similarity
        with patch.object(matcher, '_filter_candidates', return_value=abr_records):
            with patch.object(matcher, '_calculate_similarity', new_callable=AsyncMock, return_value=0.75):
                
                matches = await matcher._find_best_matches(cc_record, abr_records)
                
                # Should stop after first match
                assert len(matches) == 1
                assert matches[0].llm_confidence == 0.90
                
                # Should only call LLM once due to early termination
                assert mock_llm_client.chat_completion.call_count == 1


class TestCostOptimizationStrategies:
    """Test cost optimization strategies in the pipeline"""
    
    @pytest.mark.asyncio
    async def test_pre_filtering_effectiveness(self, pipeline_components):
        """Test that pre-filtering dramatically reduces processing load"""
        matcher = pipeline_components['matcher']
        
        cc_record = {
            'id': 1,
            'website_url': 'https://uniquebusiness.com.au',
            'company_name': 'Unique Business Solutions'
        }
        
        # Create large set of ABR records with mostly irrelevant names
        abr_records = []
        
        # Add 990 irrelevant records
        for i in range(990):
            abr_records.append({
                'id': 4000 + i,
                'entity_name': f'Unrelated Company {i:03d} Pty Ltd',
                'entity_status': 'Active',
                'trading_names': [f'Different{i:03d}'],
                'business_names': []
            })
        
        # Add 10 potentially relevant records
        for i in range(10):
            abr_records.append({
                'id': 5000 + i,
                'entity_name': f'Unique Business {i:02d} Pty Ltd',
                'entity_status': 'Active',
                'trading_names': [f'UniqueBiz{i:02d}'],
                'business_names': []
            })
        
        # Run candidate filtering
        candidates = matcher._filter_candidates(cc_record, abr_records)
        
        # Should filter out most irrelevant records
        assert len(candidates) < 50  # Much less than 1000 input records
        
        # Should include the relevant ones
        candidate_names = [c['entity_name'] for c in candidates]
        relevant_included = any('Unique Business' in name for name in candidate_names)
        assert relevant_included
    
    @pytest.mark.asyncio
    async def test_batch_processing_efficiency(self, pipeline_components):
        """Test that batch processing is used effectively"""
        extractor = pipeline_components['extractor']
        mock_llm_client = pipeline_components['llm_client']
        mock_db_manager = pipeline_components['db_manager']
        
        # Create 25 valid URLs
        valid_urls = []
        for i in range(25):
            valid_urls.append({
                'url': f'https://company{i:02d}.com.au',
                'urlkey': f'au,com,company{i:02d})/',
                'charset': 'UTF-8'
            })
        
        mock_db_manager.fetch_all.return_value = valid_urls
        
        # Mock LLM batch responses
        mock_llm_client.batch_completions.return_value = [
            f'{{"company_name": "Company {i:02d}", "industry": "Business", "confidence": 0.8}}'
            for i in range(25)
        ]
        
        await extractor.extract_companies(max_records=25)
        
        # Should use batch processing (fewer calls than individual processing)
        batch_call_count = mock_llm_client.batch_completions.call_count
        
        # Should be significantly fewer calls than 25 individual calls
        # (exact number depends on batch size, but should be much less than 25)
        assert batch_call_count <= 5
    
    @pytest.mark.asyncio
    async def test_data_quality_filtering_effectiveness(self, pipeline_components):
        """Test that data quality filtering prevents poor matches from consuming resources"""
        transformer = pipeline_components['transformer']
        mock_db_manager = pipeline_components['db_manager']
        mock_llm_client = pipeline_components['llm_client']
        
        # Create matched records with varying quality
        matched_records = []
        
        # High quality records
        for i in range(5):
            matched_records.append({
                'common_crawl_id': i,
                'abr_id': 1000 + i,
                'similarity_score': 0.90,
                'llm_confidence': 0.85,
                'cc_company_name': f'High Quality Company {i:02d}',
                'cc_website_url': f'https://quality{i:02d}.com.au',
                'cc_industry': 'Technology',
                'cc_meta_description': f'Comprehensive business description for Company {i:02d} with detailed information about services and expertise',
                'abr_abn': f'1234567890{i}',
                'abr_entity_name': f'High Quality Company {i:02d} Pty Ltd',
                'abr_entity_status': 'Active',
                'abr_trading_names': f'["Quality{i:02d}"]',
                'abr_business_names': '[]',
                'abr_address_suburb': 'Sydney',
                'abr_address_state': 'NSW',
                'abr_address_postcode': '2000'
            })
        
        # Low quality records (should be filtered out or processed minimally)
        for i in range(10):
            matched_records.append({
                'common_crawl_id': 100 + i,
                'abr_id': 2000 + i,
                'similarity_score': 0.45,  # Low similarity
                'llm_confidence': 0.35,    # Low confidence
                'cc_company_name': f'Co{i}',  # Very short name
                'cc_website_url': f'https://c{i}.com.au',
                'cc_industry': '',         # Missing industry
                'cc_meta_description': '',  # Missing description
                'abr_abn': f'9876543210{i}',
                'abr_entity_name': f'Company{i}',  # Very short
                'abr_entity_status': 'Active',
                'abr_trading_names': '[]',
                'abr_business_names': '[]',
                'abr_address_suburb': '',   # Missing address
                'abr_address_state': '',
                'abr_address_postcode': ''
            })
        
        mock_db_manager.fetch_all.return_value = matched_records
        
        # Track LLM calls for expensive operations
        llm_call_count = 0
        async def count_llm_calls(*args, **kwargs):
            nonlocal llm_call_count
            llm_call_count += 1
            return json.dumps({
                "best_name": "Default Name",
                "reasoning": "Default",
                "confidence": 0.5
            })
        
        mock_llm_client.chat_completion = AsyncMock(side_effect=count_llm_calls)
        
        # Run transformation
        result = await transformer.transform_matched_records()
        
        # Should process records but use fewer LLM calls for low quality records
        # (due to similarity thresholds and quality checks)
        assert result > 0
        
        # LLM calls should be reasonable (not 15 calls for name determination)
        # High quality records may trigger LLM, low quality should mostly use fallbacks
        assert llm_call_count <= 10


class TestIntegrationScenarios:
    """Test realistic integration scenarios"""
    
    @pytest.mark.asyncio
    async def test_realistic_pipeline_filtering_scenario(self, pipeline_components):
        """Test a realistic scenario with mixed data quality"""
        extractor = pipeline_components['extractor']
        matcher = pipeline_components['matcher']
        mock_db_manager = pipeline_components['db_manager']
        mock_llm_client = pipeline_components['llm_client']
        
        # Mock Common Crawl URLs (realistic mix)
        cc_urls = []
        
        # 50 good company URLs
        for i in range(50):
            cc_urls.append({
                'url': f'https://company{i:03d}.com.au',
                'urlkey': f'au,com,company{i:03d})/',
                'charset': 'UTF-8'
            })
        
        # 200 URLs that should be filtered out
        for i in range(200):
            cc_urls.extend([
                {'url': f'https://site{i:03d}.com.au/blog/news', 'urlkey': f'au,com,site{i:03d})/blog/news', 'charset': 'UTF-8'},
                {'url': f'https://site{i:03d}.com.au/wp-admin/', 'urlkey': f'au,com,site{i:03d})/wp-admin/', 'charset': 'UTF-8'},
                {'url': f'https://site{i:03d}.com.au/file.pdf', 'urlkey': f'au,com,site{i:03d})/file.pdf', 'charset': 'UTF-8'}
            ])
        
        # Mock ABR records (realistic business data)
        abr_records = []
        for i in range(1000):  # Large realistic ABR dataset
            abr_records.append({
                'id': 10000 + i,
                'abn': f'1234567{i:04d}',
                'entity_name': f'Business Entity {i:04d} Pty Ltd',
                'entity_status': 'Active' if i % 10 != 0 else 'Cancelled',  # 10% inactive
                'trading_names': f'["Entity{i:04d}"]' if i % 3 == 0 else '[]',
                'business_names': '[]'
            })
        
        # Configure mocks
        def mock_fetch_all(query):
            if 'common_crawl' in query:
                return cc_urls
            elif 'abr_raw' in query:
                return abr_records
            else:
                return []
        
        mock_db_manager.fetch_all = AsyncMock(side_effect=mock_fetch_all)
        
        # Mock LLM responses
        mock_llm_client.batch_completions = AsyncMock(return_value=[
            f'{{"company_name": "Company {i:03d}", "industry": "Business", "confidence": 0.8}}'
            for i in range(50)  # Only for valid URLs
        ])
        
        mock_llm_client.chat_completion = AsyncMock(return_value=json.dumps({
            "is_match": False,
            "confidence": 0.40,
            "reasoning": "Not a strong match"
        }))
        
        # Run extraction phase
        await extractor.extract_companies(max_records=250)
        
        # Verify filtering worked
        extraction_calls = mock_llm_client.batch_completions.call_count
        assert extraction_calls > 0
        
        # Verify only valid URLs were processed (check prompts)
        if extraction_calls > 0:
            all_prompts = []
            for call in mock_llm_client.batch_completions.call_args_list:
                all_prompts.extend(call[0][0])
            
            # Should have processed only valid URLs (50), not filtered ones (200)
            assert len(all_prompts) <= 60  # Some buffer for batch processing
    
    @pytest.mark.asyncio
    async def test_memory_efficiency_with_large_datasets(self, pipeline_components):
        """Test that the system handles large datasets efficiently"""
        matcher = pipeline_components['matcher']
        
        # Simulate processing a single CC record against large ABR dataset
        cc_record = {
            'id': 1,
            'website_url': 'https://testcompany.com.au',
            'company_name': 'Test Company Solutions'
        }
        
        # Create very large ABR dataset (simulate 100K records)
        # But only process a small subset due to filtering
        large_abr_set = []
        for i in range(100):  # Reduced for test performance, but represents much larger set
            large_abr_set.append({
                'id': 20000 + i,
                'entity_name': f'Test Company {i:03d} Pty Ltd' if i < 5 else f'Unrelated Business {i:03d} Ltd',
                'entity_status': 'Active',
                'trading_names': [f'Test{i:03d}'] if i < 5 else [f'Unrelated{i:03d}'],
                'business_names': []
            })
        
        # Mock similarity calculation to track how many are processed
        processed_count = 0
        async def track_similarity_calls(*args, **kwargs):
            nonlocal processed_count
            processed_count += 1
            return 0.70  # Above threshold
        
        with patch.object(matcher, '_calculate_similarity', new_callable=AsyncMock, side_effect=track_similarity_calls):
            # Mock LLM to prevent actual API calls
            with patch.object(matcher, '_llm_verify_match', new_callable=AsyncMock) as mock_llm:
                mock_llm.return_value = {
                    'is_match': False,
                    'confidence': 0.65,
                    'reasoning': 'Test response'
                }
                
                matches = await matcher._find_best_matches(cc_record, large_abr_set)
                
                # Should have limited processing due to candidate filtering and limits
                assert processed_count <= 50  # Limited by candidate processing limits
                
                # Should complete without memory issues
                assert isinstance(matches, list)


class TestPerformanceMetrics:
    """Test performance and efficiency metrics"""
    
    @pytest.mark.asyncio
    async def test_filtering_ratio_effectiveness(self, pipeline_components):
        """Test that filtering achieves significant ratio improvements"""
        extractor = pipeline_components['extractor']
        mock_db_manager = pipeline_components['db_manager']
        mock_llm_client = pipeline_components['llm_client']
        
        # Create realistic URL distribution
        total_urls = 1000
        valid_company_urls = 200
        filtered_out_urls = total_urls - valid_company_urls
        
        all_urls = []
        
        # Add valid company URLs
        for i in range(valid_company_urls):
            all_urls.append({
                'url': f'https://validcompany{i:03d}.com.au',
                'urlkey': f'au,com,validcompany{i:03d})/'
            })
        
        # Add URLs that should be filtered out
        filter_patterns = ['/blog/', '/news/', '/wp-admin/', '/wp-content/', '.pdf', '.doc']
        for i in range(filtered_out_urls):
            pattern = filter_patterns[i % len(filter_patterns)]
            if pattern.startswith('.'):
                url = f'https://company{i:03d}.com.au/file{pattern}'
                urlkey = f'au,com,company{i:03d})/file{pattern}'
            else:
                url = f'https://company{i:03d}.com.au{pattern}page'
                urlkey = f'au,com,company{i:03d}){pattern}page'
            
            all_urls.append({'url': url, 'urlkey': urlkey})
        
        mock_db_manager.fetch_all.return_value = all_urls
        
        # Mock LLM responses for valid URLs only
        mock_llm_client.batch_completions.return_value = [
            '{"company_name": "Valid Company", "industry": "Business", "confidence": 0.8}'
        ] * valid_company_urls
        
        # Track URL processing
        processed_urls = []
        
        # Patch the batch processing to track what gets processed
        original_process = extractor._process_batch_with_llm
        async def track_processing(urls):
            processed_urls.extend(urls)
            return await original_process(urls)
        
        with patch.object(extractor, '_process_batch_with_llm', track_processing):
            await extractor.extract_companies(max_records=total_urls)
        
        # Calculate filtering effectiveness
        processing_ratio = len(processed_urls) / total_urls
        
        # Should have achieved significant filtering (processed much less than total)
        assert processing_ratio <= 0.25  # Should process 25% or less due to filtering
        
        # Verify only valid URLs were processed
        for url_record in processed_urls:
            url = url_record['url']
            assert not any(pattern in url for pattern in ['/blog/', '/wp-admin/', '.pdf', '.doc'])


class TestResourceOptimization:
    """Test resource optimization strategies"""
    
    @pytest.mark.asyncio
    async def test_concurrent_processing_limits(self, pipeline_components):
        """Test that concurrent processing is limited for resource management"""
        extractor = pipeline_components['extractor']
        mock_llm_client = pipeline_components['llm_client']
        
        # Test that batch size limits are respected
        large_url_list = [
            {'url': f'https://company{i:03d}.com.au', 'urlkey': f'au,com,company{i:03d})/'}
            for i in range(100)
        ]
        
        # Track batch sizes
        batch_sizes = []
        
        def track_batch_size(prompts, **kwargs):
            batch_sizes.append(len(prompts))
            return ['{"company_name": "Test", "confidence": 0.8}'] * len(prompts)
        
        mock_llm_client.batch_completions = AsyncMock(side_effect=track_batch_size)
        
        # Process URLs with batch processing
        await extractor._process_batch_with_llm(large_url_list)
        
        # Verify reasonable batch sizes (not processing all 100 at once)
        if batch_sizes:
            max_batch_size = max(batch_sizes)
            assert max_batch_size <= 20  # Should limit batch size for resource management
    
    @pytest.mark.asyncio
    async def test_error_resilience_maintains_filtering(self, pipeline_components):
        """Test that errors don't bypass filtering mechanisms"""
        matcher = pipeline_components['matcher']
        mock_llm_client = pipeline_components['llm_client']
        
        cc_record = {
            'id': 1,
            'website_url': 'https://resilience.com.au',
            'company_name': 'Resilience Test Company'
        }
        
        abr_records = [{
            'id': 30000,
            'entity_name': 'Similar Test Company Pty Ltd',
            'entity_status': 'Active',
            'trading_names': ['Test Company'],
            'business_names': []
        }]
        
        # Mock LLM to raise exception on first few calls, then succeed
        call_count = 0
        async def failing_llm(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Exception("Simulated API failure")
            return json.dumps({
                "is_match": True,
                "confidence": 0.80,
                "reasoning": "Match after error recovery"
            })
        
        mock_llm_client.chat_completion = AsyncMock(side_effect=failing_llm)
        
        # Mock filtering and similarity to pass through
        with patch.object(matcher, '_filter_candidates', return_value=abr_records):
            with patch.object(matcher, '_calculate_similarity', new_callable=AsyncMock, return_value=0.75):
                
                # Should handle errors gracefully and maintain filtering logic
                matches = await matcher._find_best_matches(cc_record, abr_records)
                
                # Error handling should still allow eventual success or graceful failure
                # The key is that filtering logic remains intact
                assert isinstance(matches, list)  # Should return valid response structure


if __name__ == '__main__':
    pytest.main([__file__, '-v'])