"""Token counting utility for AI service budget management.

This module provides token counting functionality using tiktoken library
to ensure API requests stay within budget limits.
"""

import logging
from typing import Tuple, Optional
import tiktoken


class TokenCounter:
    """Count tokens for OpenAI API requests.
    
    Uses tiktoken library to accurately count tokens before sending
    requests to the API, enabling proactive budget management.
    """
    
    def __init__(self, model: str = "gpt-4"):
        """Initialize token counter.
        
        Args:
            model: OpenAI model name for token encoding
        """
        self.model = model
        self.logger = logging.getLogger(__name__)
        
        # Get appropriate encoding for model
        try:
            self.encoding = tiktoken.encoding_for_model(model)
        except KeyError:
            self.logger.warning(f"Model {model} not found, using cl100k_base encoding")
            self.encoding = tiktoken.get_encoding("cl100k_base")
    
    def count_tokens(self, text: str) -> int:
        """Count tokens in text.
        
        Args:
            text: Text to count tokens for
            
        Returns:
            Number of tokens in text
        """
        if not text:
            return 0
        
        try:
            tokens = self.encoding.encode(text)
            return len(tokens)
        except Exception as e:
            self.logger.error(f"Error counting tokens: {e}")
            # Fallback: rough estimate (1 token ≈ 4 characters)
            return len(text) // 4
    
    def count_messages_tokens(self, messages: list[dict]) -> int:
        """Count tokens in chat messages.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            
        Returns:
            Total token count for all messages
        """
        total_tokens = 0
        
        # Add tokens for message formatting
        # Every message follows <|start|>{role/name}\n{content}<|end|>\n
        for message in messages:
            total_tokens += 4  # Message formatting overhead
            
            for key, value in message.items():
                if isinstance(value, str):
                    total_tokens += self.count_tokens(value)
                    
                if key == "name":
                    total_tokens += -1  # Role is always required and always 1 token
        
        total_tokens += 2  # Every reply is primed with <|start|>assistant
        
        return total_tokens
    
    def estimate_response_tokens(self, prompt_tokens: int, max_ratio: float = 0.5) -> int:
        """Estimate expected response tokens based on prompt size.
        
        Args:
            prompt_tokens: Number of tokens in prompt
            max_ratio: Expected response to prompt ratio (default 0.5)
            
        Returns:
            Estimated response token count
        """
        estimated = int(prompt_tokens * max_ratio)
        
        # Minimum reasonable response
        return max(estimated, 100)
    
    def fits_in_budget(
        self, 
        text: str, 
        budget: int, 
        include_response_estimate: bool = True
    ) -> Tuple[bool, int]:
        """Check if text fits within token budget.
        
        Args:
            text: Text to check
            budget: Maximum allowed tokens
            include_response_estimate: If True, reserve space for response
            
        Returns:
            Tuple of (fits_in_budget, actual_token_count)
        """
        token_count = self.count_tokens(text)
        
        if include_response_estimate:
            response_estimate = self.estimate_response_tokens(token_count)
            total_needed = token_count + response_estimate
        else:
            total_needed = token_count
        
        fits = total_needed <= budget
        
        self.logger.debug(
            f"Token budget check: {total_needed}/{budget} tokens "
            f"({'PASS' if fits else 'FAIL'})"
        )
        
        return fits, token_count
    
    def get_budget_remaining(
        self, 
        used_tokens: int, 
        total_budget: int
    ) -> Tuple[int, float]:
        """Calculate remaining token budget.
        
        Args:
            used_tokens: Tokens already used
            total_budget: Total token budget
            
        Returns:
            Tuple of (remaining_tokens, percentage_used)
        """
        remaining = max(0, total_budget - used_tokens)
        percentage = (used_tokens / total_budget * 100) if total_budget > 0 else 100.0
        
        return remaining, percentage
