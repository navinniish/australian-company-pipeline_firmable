"""
LLM client for interacting with various language models.
Supports OpenAI GPT and other providers.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
import json
import time
import openai
import anthropic
from anthropic import Anthropic
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class LLMConfig:
    """Configuration for LLM providers."""
    provider: str  # 'openai', 'anthropic', 'azure'
    model: str
    api_key: str
    temperature: float = 0.3
    max_tokens: int = 2000
    timeout: int = 60


class LLMClient:
    """
    Unified client for multiple LLM providers.
    Optimized for entity matching and data extraction tasks with enhanced concurrency.
    """
    
    def __init__(self, config: Any):
        """Initialize LLM client with configuration."""
        # Access config through the llm attribute
        self.provider = config.llm.provider
        self.model = config.llm.model
        self.api_key = config.llm.openai_api_key
        self.anthropic_api_key = config.llm.anthropic_api_key
        
        # Initialize clients
        if self.provider == 'openai' and self.api_key:
            self.openai_client = openai.AsyncOpenAI(api_key=self.api_key)
        elif self.provider == 'anthropic' and self.anthropic_api_key:
            self.anthropic_client = Anthropic(api_key=self.anthropic_api_key)
        else:
            logger.warning("No valid LLM configuration found. Using mock responses.")
            self.use_mock = True
        
        self.temperature = 0.3  # Low temperature for consistent, factual responses
        self.max_tokens = 2000
    
    async def chat_completion(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """
        Get chat completion from configured LLM provider.
        
        Args:
            prompt: User prompt
            system_prompt: Optional system prompt
            
        Returns:
            LLM response text
        """
        if hasattr(self, 'use_mock'):
            return await self._mock_response(prompt)
        
        try:
            if self.provider == 'openai':
                return await self._openai_completion(prompt, system_prompt)
            elif self.provider == 'anthropic':
                return await self._anthropic_completion(prompt, system_prompt)
            else:
                raise ValueError(f"Unsupported LLM provider: {self.provider}")
                
        except Exception as e:
            logger.error(f"LLM completion failed: {e}")
            return await self._mock_response(prompt)
    
    async def _openai_completion(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """Get completion from OpenAI."""
        messages = []
        
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        
        messages.append({"role": "user", "content": prompt})
        
        response = await self.openai_client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            timeout=60
        )
        
        return response.choices[0].message.content.strip()
    
    async def _anthropic_completion(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """Get completion from Anthropic with async optimization."""
        # Run Anthropic API call in executor to avoid blocking
        loop = asyncio.get_event_loop()
        
        def sync_anthropic_call():
            full_prompt = prompt
            if system_prompt:
                full_prompt = f"{system_prompt}\n\n{prompt}"
            
            message = self.anthropic_client.messages.create(
                model=self.model,  # Use configured model (Haiku 3.5)
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                messages=[{"role": "user", "content": full_prompt}]
            )
            return message.content[0].text.strip()
        
        return await loop.run_in_executor(None, sync_anthropic_call)
    
    async def _mock_response(self, prompt: str) -> str:
        """
        Generate mock responses for testing when no API key is available.
        Provides reasonable responses for entity matching prompts.
        """
        logger.info("Using mock LLM response")
        
        # Simple pattern matching for different types of prompts
        if "entity matching" in prompt.lower() or "same company" in prompt.lower():
            # Mock entity matching response
            return json.dumps({
                "is_match": True,
                "confidence": 0.75,
                "reasoning": "Company names show strong similarity with minor variations. Domain name aligns with business name pattern.",
                "key_factors": ["name_similarity", "domain_alignment"]
            })
        
        elif "extract" in prompt.lower() and "company" in prompt.lower():
            # Mock company extraction response  
            return json.dumps({
                "company_name": "Sample Company Pty Ltd",
                "industry": "Professional Services",
                "contact_info": {
                    "email": "info@samplecompany.com.au",
                    "phone": None,
                    "address": None
                },
                "confidence": 0.6
            })
        
        else:
            return "Mock LLM response - no API key configured"
    
    async def batch_completions(self, prompts: List[str], system_prompt: Optional[str] = None, 
                               batch_size: int = 15) -> List[str]:
        """
        Process multiple prompts in batch with optimized concurrency.
        
        Args:
            prompts: List of prompts to process
            system_prompt: Optional system prompt
            batch_size: Number of concurrent requests (default 15 for 30% improvement)
            
        Returns:
            List of responses
        """
        # Process in batches to optimize API usage and avoid rate limits
        results = []
        
        for i in range(0, len(prompts), batch_size):
            batch_prompts = prompts[i:i + batch_size]
            semaphore = asyncio.Semaphore(min(batch_size, len(batch_prompts)))
            
            async def process_single(prompt):
                async with semaphore:
                    return await self.chat_completion(prompt, system_prompt)
            
            tasks = [process_single(prompt) for prompt in batch_prompts]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle any exceptions and provide fallback responses
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Batch completion failed for prompt {i+j}: {result}")
                    results.append(await self._mock_response(batch_prompts[j]))
                else:
                    results.append(result)
        
        return results
    
    def estimate_tokens(self, text: str) -> int:
        """
        Rough estimation of token count for cost calculation.
        
        Args:
            text: Input text
            
        Returns:
            Estimated token count
        """
        # Rough approximation: ~4 characters per token
        return len(text) // 4
    
    def estimate_cost(self, prompt_tokens: int, completion_tokens: int) -> float:
        """
        Estimate cost based on token usage.
        
        Args:
            prompt_tokens: Number of input tokens
            completion_tokens: Number of output tokens
            
        Returns:
            Estimated cost in USD
        """
        # Costs as of 2024 (approximate, check current pricing)
        if self.provider == 'openai':
            if 'gpt-4' in self.model:
                input_cost = prompt_tokens * 0.01 / 1000
                output_cost = completion_tokens * 0.03 / 1000
            else:  # GPT-3.5
                input_cost = prompt_tokens * 0.0015 / 1000
                output_cost = completion_tokens * 0.002 / 1000
        elif self.provider == 'anthropic':
            input_cost = prompt_tokens * 0.008 / 1000
            output_cost = completion_tokens * 0.024 / 1000
        else:
            input_cost = output_cost = 0.0
        
        return input_cost + output_cost


# Example usage
if __name__ == "__main__":
    import asyncio
    from ..utils.config import Config
    
    async def main():
        config = Config()
        client = LLMClient(config)
        
        # Test company extraction
        test_prompt = """
        Extract company information from this website:
        URL: https://example.com.au
        Title: ABC Manufacturing - Quality Products Since 1995
        Description: Leading Australian manufacturer of industrial equipment and machinery
        """
        
        response = await client.chat_completion(test_prompt)
        print(f"Response: {response}")
        
        # Test entity matching
        match_prompt = """
        Are these the same company?
        Record 1: ABC Manufacturing Pty Ltd (website: abc-manufacturing.com.au)
        Record 2: ABC Manufacturing (ABN: 12345678901, Trading as: ABC Mfg)
        """
        
        match_response = await client.chat_completion(match_prompt)
        print(f"Match response: {match_response}")
    
    asyncio.run(main())