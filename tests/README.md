# Australian Company Pipeline - Test Suite

This directory contains comprehensive tests for the Australian Company Pipeline's filtering logic, LLM integration, and cost optimization strategies.

## Test Structure

### Core Test Files

- **`test_entity_filtering.py`** - Tests for entity matching filtering logic
- **`test_llm_verification.py`** - Tests for LLM verification and response handling  
- **`test_url_filtering.py`** - Tests for URL filtering in Common Crawl extraction
- **`test_similarity_calculations.py`** - Tests for similarity calculation methods
- **`test_data_transformation.py`** - Tests for data transformation logic
- **`test_integration_filtering.py`** - End-to-end integration tests for filtering

### Configuration Files

- **`conftest.py`** - Pytest configuration and shared fixtures
- **`README.md`** - This documentation file

## What These Tests Validate

### 1. Filtering Logic Efficiency (`test_entity_filtering.py`)

Tests that verify the cost-optimization filtering strategies:

- **Candidate filtering** reduces 1M ABR records to ~50 candidates per Common Crawl record
- **Entity status filtering** excludes inactive businesses (20-30% reduction)
- **Domain-name similarity** pre-screening catches obvious matches
- **Quick name similarity** threshold (≥70%) for initial filtering
- **Confidence thresholds** prevent unnecessary LLM calls:
  - Manual review: 40%
  - LLM review: 60%  
  - High confidence: 85%
  - Exact match: 95%

### 2. LLM Verification Logic (`test_llm_verification.py`)

Tests the LLM integration and cost controls:

- **Prompt construction** includes all relevant fields from both datasets
- **Response parsing** handles JSON validation and error cases
- **Confidence validation** clamps scores to 0.0-1.0 range
- **Error handling** provides safe fallbacks when LLM calls fail
- **Early termination** stops processing after first confirmed match
- **Threshold enforcement** prevents LLM calls below similarity thresholds

### 3. URL Filtering (`test_url_filtering.py`)

Tests the Common Crawl URL pre-processing that achieves 40-60% cost reduction:

- **Australian domain identification** (.com.au, .net.au, etc.)
- **Excluded path patterns**:
  - `/blog/`, `/news/`, `/articles/` 
  - `/wp-admin/`, `/wp-content/`
  - `/user/`, `/member/`, `/profile/`
- **File extension filtering** (.pdf, .doc, .jpg, .zip, .exe)
- **Subdomain handling** and normalization
- **Malformed URL rejection**

### 4. Similarity Calculations (`test_similarity_calculations.py`)

Tests the multi-factor similarity scoring system:

- **Name similarity** (50% weight) - sequence matching + token overlap
- **Semantic similarity** (20% weight) - sentence embeddings with cosine similarity
- **Location similarity** (15% weight) - currently neutral scoring
- **Industry similarity** (15% weight) - keyword mapping
- **Trading/business name alternatives** consideration
- **Unicode and edge case handling**

### 5. Data Transformation (`test_data_transformation.py`)

Tests the LLM-enhanced data cleaning and standardization:

- **Name determination** - uses LLM when similarity < 90%
- **Industry classification** - LLM for complex descriptions (>20 chars)
- **Data quality scoring** based on completeness and confidence
- **Contact information extraction** using regex patterns
- **Address standardization** and formatting
- **Alternative name processing** with deduplication

### 6. Integration Testing (`test_integration_filtering.py`)

End-to-end tests validating the complete filtering pipeline:

- **Realistic data volumes** - 1000+ URLs, 1000+ ABR records
- **Filtering ratio effectiveness** - processes <25% of input URLs
- **Memory efficiency** with large datasets
- **Batch processing** limits and resource management
- **Error resilience** maintaining filtering under failure conditions
- **Performance metrics** and cost optimization validation

## Key Test Scenarios

### Cost Optimization Validation

The tests verify that the pipeline achieves **70-85% cost reduction** through:

1. **Pre-filtering effectiveness**: 
   - URL filtering: 40-60% reduction
   - Entity status filtering: 20-30% reduction  
   - Domain similarity: 50% reduction in entity matching

2. **Confidence threshold enforcement**:
   - <40%: Skip entirely
   - 40-60%: Manual review queue
   - 60-85%: LLM verification
   - >85%: Auto-accept

3. **Processing limits**:
   - Max 50 candidates per similarity calculation
   - Max 5 candidates per LLM review
   - Early termination on first match

4. **Batch optimization**:
   - 15x concurrent LLM requests
   - Reasonable batch sizes (≤20)
   - Memory-efficient processing

### Quality Assurance

The tests ensure filtering doesn't compromise quality:

- **High-quality matches preserved** through multi-stage scoring
- **Edge cases handled** (Unicode, long names, missing data)
- **Error recovery** maintains pipeline integrity
- **Fallback mechanisms** provide safe defaults

## Running the Tests

### Prerequisites

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Ensure the src directory is in Python path
export PYTHONPATH="${PYTHONPATH}:./src"
```

### Running Individual Test Files

```bash
# Test entity filtering logic
pytest tests/test_entity_filtering.py -v

# Test LLM verification
pytest tests/test_llm_verification.py -v

# Test URL filtering
pytest tests/test_url_filtering.py -v

# Test similarity calculations
pytest tests/test_similarity_calculations.py -v

# Test data transformation
pytest tests/test_data_transformation.py -v

# Test integration scenarios
pytest tests/test_integration_filtering.py -v
```

### Running All Tests

```bash
# Run complete test suite
pytest tests/ -v

# Run with coverage report
pytest tests/ --cov=src --cov-report=html

# Run performance tests only
pytest tests/ -k "performance" -v

# Run filtering tests only  
pytest tests/ -k "filtering" -v
```

### Test Configuration

Tests use mocked LLM and database clients to:
- **Avoid API costs** during testing
- **Control response scenarios** for edge case testing
- **Ensure deterministic results** across test runs
- **Test error conditions** safely

## Performance Benchmarks

The tests validate these performance targets:

| Metric | Target | Validation |
|--------|--------|------------|
| URL filtering ratio | >60% filtered out | `test_url_filtering_effectiveness` |
| Entity matching candidates | ≤50 per CC record | `test_candidate_limit_top_50` |
| LLM verification limit | ≤5 per CC record | `test_llm_review_limit_top_5` |
| Early termination | Stop on first match | `test_early_termination_saves_llm_calls` |
| Batch processing | ≤20 concurrent requests | `test_concurrent_processing_limits` |
| Memory efficiency | Handle 100K+ records | `test_memory_efficiency_with_large_datasets` |

## Test Coverage

The test suite covers:

- **Filtering logic**: 95%+ coverage of all filtering functions
- **LLM integration**: All response scenarios and error cases
- **Cost optimizations**: All efficiency measures and thresholds
- **Data quality**: Edge cases and error handling
- **Integration**: End-to-end pipeline scenarios

## Continuous Integration

These tests are designed to run in CI/CD pipelines:

- **No external dependencies** (mocked LLM/DB calls)
- **Deterministic results** for reliable builds
- **Fast execution** (typically <2 minutes for full suite)
- **Clear failure reporting** with detailed assertion messages

## Contributing

When adding new filtering logic or cost optimizations:

1. **Add corresponding tests** that validate the efficiency gains
2. **Include performance assertions** with specific metrics
3. **Test error scenarios** to ensure robustness
4. **Update documentation** to reflect new optimizations
5. **Run full test suite** to ensure no regressions

The test suite serves as both validation and documentation of the pipeline's sophisticated cost optimization strategies.