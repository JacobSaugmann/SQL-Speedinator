"""Token budget enforcement and data reduction for AI service.

This module manages token budgets and provides intelligent data reduction
to ensure API requests stay within budget constraints.
"""

import logging
import json
from typing import Dict, Any, List, Tuple
from enum import Enum

from .token_counter import TokenCounter


class DataPriority(Enum):
    """Priority levels for performance data sections."""
    CRITICAL = 1  # Must always include
    HIGH = 2      # Include if budget allows
    MEDIUM = 3    # Include if sufficient budget
    LOW = 4       # Optional, include only if plenty of budget


class TokenBudget:
    """Enforce token budget limits with intelligent data reduction.
    
    Manages token budgets across different sections of AI requests,
    reducing data by priority when necessary to fit within limits.
    """
    
    # Token budget allocation
    BUDGETS = {
        'system_prompt': 500,      # System prompt max tokens
        'performance_data': 2500,  # Performance data max tokens
        'response': 1500,          # Expected response tokens
        'total_max': 4500          # Hard limit for entire request
    }
    
    # Priority mapping for data sections
    SECTION_PRIORITIES = {
        # CRITICAL - Always include these
        'wait_stats': DataPriority.CRITICAL,
        'disk_issues': DataPriority.CRITICAL,
        'high_latency': DataPriority.CRITICAL,
        
        # HIGH - Include unless severely over budget
        'index_issues': DataPriority.HIGH,
        'missing_indexes': DataPriority.HIGH,
        'fragmented_indexes': DataPriority.HIGH,
        
        # MEDIUM - Include if budget allows
        'config_issues': DataPriority.MEDIUM,
        'tempdb_issues': DataPriority.MEDIUM,
        'server_info': DataPriority.MEDIUM,
        
        # LOW - Optional extras
        'plan_cache': DataPriority.LOW,
        'database_info': DataPriority.LOW,
        'backup_info': DataPriority.LOW
    }
    
    def __init__(self, token_counter: TokenCounter = None):
        """Initialize token budget manager.
        
        Args:
            token_counter: TokenCounter instance for counting tokens
        """
        self.token_counter = token_counter or TokenCounter()
        self.logger = logging.getLogger(__name__)
    
    def check_budget(self, data: Dict[str, Any]) -> Tuple[bool, int, str]:
        """Check if data fits within budget.
        
        Args:
            data: Performance data to check
            
        Returns:
            Tuple of (fits_budget, token_count, reason)
        """
        data_str = json.dumps(data, default=str)
        token_count = self.token_counter.count_tokens(data_str)
        
        fits = token_count <= self.BUDGETS['performance_data']
        
        if fits:
            reason = f"Within budget: {token_count}/{self.BUDGETS['performance_data']} tokens"
        else:
            overage = token_count - self.BUDGETS['performance_data']
            reason = f"Over budget by {overage} tokens ({token_count}/{self.BUDGETS['performance_data']})"
        
        self.logger.info(f"Budget check: {reason}")
        
        return fits, token_count, reason
    
    def reduce_to_budget(
        self, 
        data: Dict[str, Any], 
        target_tokens: int = None
    ) -> Dict[str, Any]:
        """Reduce data to fit within target token budget.
        
        Uses priority-based filtering to keep most important data
        while removing less critical information.
        
        Args:
            data: Performance data to reduce
            target_tokens: Target token count (default: performance_data budget)
            
        Returns:
            Reduced data dictionary fitting within budget
        """
        if target_tokens is None:
            target_tokens = self.BUDGETS['performance_data']
        
        # Check if already within budget
        fits, current_tokens, _ = self.check_budget(data)
        if fits:
            self.logger.info("Data already within budget, no reduction needed")
            return data
        
        self.logger.warning(
            f"Data exceeds budget ({current_tokens} tokens), "
            f"reducing to {target_tokens} tokens"
        )
        
        # Reduce by priority levels
        reduced_data = {}
        
        # CRITICAL - Always include
        reduced_data.update(self._extract_priority_sections(data, DataPriority.CRITICAL))
        
        # Check if we have room for HIGH priority
        current_size = self._get_token_count(reduced_data)
        if current_size < target_tokens * 0.6:  # Reserve 60% for critical+high
            reduced_data.update(self._extract_priority_sections(data, DataPriority.HIGH))
        
        # Check if we have room for MEDIUM priority
        current_size = self._get_token_count(reduced_data)
        if current_size < target_tokens * 0.8:  # Reserve 80% for critical+high+medium
            reduced_data.update(self._extract_priority_sections(data, DataPriority.MEDIUM))
        
        # Add LOW priority only if plenty of room
        current_size = self._get_token_count(reduced_data)
        if current_size < target_tokens * 0.9:  # Leave 10% margin
            reduced_data.update(self._extract_priority_sections(data, DataPriority.LOW))
        
        # Final check and aggressive reduction if still over
        final_size = self._get_token_count(reduced_data)
        if final_size > target_tokens:
            reduced_data = self._aggressive_reduction(reduced_data, target_tokens)
        
        final_size = self._get_token_count(reduced_data)
        self.logger.info(
            f"Reduced data from {current_tokens} to {final_size} tokens "
            f"(target: {target_tokens})"
        )
        
        return reduced_data
    
    def _extract_priority_sections(
        self, 
        data: Dict[str, Any], 
        priority: DataPriority
    ) -> Dict[str, Any]:
        """Extract sections matching specific priority level.
        
        Args:
            data: Full data dictionary
            priority: Priority level to extract
            
        Returns:
            Dictionary containing only sections of specified priority
        """
        sections = {}
        
        for key, value in data.items():
            section_priority = self.SECTION_PRIORITIES.get(key, DataPriority.LOW)
            if section_priority == priority:
                sections[key] = value
        
        return sections
    
    def _get_token_count(self, data: Dict[str, Any]) -> int:
        """Get token count for data dictionary.
        
        Args:
            data: Data to count tokens for
            
        Returns:
            Token count
        """
        data_str = json.dumps(data, default=str)
        return self.token_counter.count_tokens(data_str)
    
    def _aggressive_reduction(
        self, 
        data: Dict[str, Any], 
        target_tokens: int
    ) -> Dict[str, Any]:
        """Aggressively reduce data by truncating large sections.
        
        Last resort when priority-based filtering isn't enough.
        
        Args:
            data: Data to reduce
            target_tokens: Target token count
            
        Returns:
            Aggressively reduced data
        """
        self.logger.warning("Applying aggressive data reduction")
        
        reduced = {}
        tokens_used = 0
        
        # Sort sections by priority
        sorted_sections = sorted(
            data.items(),
            key=lambda x: self.SECTION_PRIORITIES.get(x[0], DataPriority.LOW).value
        )
        
        for key, value in sorted_sections:
            section_str = json.dumps({key: value}, default=str)
            section_tokens = self.token_counter.count_tokens(section_str)
            
            # If section fits, add it
            if tokens_used + section_tokens <= target_tokens:
                reduced[key] = value
                tokens_used += section_tokens
            else:
                # Try to add truncated version
                remaining_tokens = target_tokens - tokens_used
                if remaining_tokens > 50:  # Only if meaningful space left
                    truncated_value = self._truncate_section(value, remaining_tokens)
                    reduced[key] = truncated_value
                    break
        
        return reduced
    
    def _truncate_section(self, value: Any, max_tokens: int) -> Any:
        """Truncate section to fit within token limit.
        
        Args:
            value: Section value to truncate
            max_tokens: Maximum tokens allowed
            
        Returns:
            Truncated value
        """
        if isinstance(value, list):
            # Keep first items up to token limit
            truncated = []
            tokens_used = 0
            
            for item in value:
                item_str = json.dumps(item, default=str)
                item_tokens = self.token_counter.count_tokens(item_str)
                
                if tokens_used + item_tokens <= max_tokens:
                    truncated.append(item)
                    tokens_used += item_tokens
                else:
                    break
            
            if len(truncated) < len(value):
                truncated.append(f"... truncated {len(value) - len(truncated)} items")
            
            return truncated
        
        elif isinstance(value, dict):
            # Keep entries up to token limit
            truncated = {}
            tokens_used = 0
            
            for k, v in value.items():
                entry_str = json.dumps({k: v}, default=str)
                entry_tokens = self.token_counter.count_tokens(entry_str)
                
                if tokens_used + entry_tokens <= max_tokens:
                    truncated[k] = v
                    tokens_used += entry_tokens
                else:
                    break
            
            return truncated
        
        elif isinstance(value, str):
            # Truncate string to approximate token limit
            # Rough estimate: 1 token ≈ 4 characters
            max_chars = max_tokens * 4
            if len(value) > max_chars:
                return value[:max_chars] + "... [truncated]"
            return value
        
        return value
    
    def get_budget_summary(
        self, 
        system_prompt_tokens: int,
        data_tokens: int,
        response_tokens: int = None
    ) -> Dict[str, Any]:
        """Get budget usage summary.
        
        Args:
            system_prompt_tokens: Tokens used by system prompt
            data_tokens: Tokens used by performance data
            response_tokens: Tokens used in response (optional)
            
        Returns:
            Budget summary dictionary
        """
        if response_tokens is None:
            response_tokens = self.token_counter.estimate_response_tokens(
                system_prompt_tokens + data_tokens
            )
        
        total_tokens = system_prompt_tokens + data_tokens + response_tokens
        
        summary = {
            'system_prompt': {
                'used': system_prompt_tokens,
                'budget': self.BUDGETS['system_prompt'],
                'percentage': (system_prompt_tokens / self.BUDGETS['system_prompt'] * 100)
            },
            'performance_data': {
                'used': data_tokens,
                'budget': self.BUDGETS['performance_data'],
                'percentage': (data_tokens / self.BUDGETS['performance_data'] * 100)
            },
            'response': {
                'used': response_tokens,
                'budget': self.BUDGETS['response'],
                'percentage': (response_tokens / self.BUDGETS['response'] * 100)
            },
            'total': {
                'used': total_tokens,
                'budget': self.BUDGETS['total_max'],
                'percentage': (total_tokens / self.BUDGETS['total_max'] * 100)
            },
            'within_budget': total_tokens <= self.BUDGETS['total_max']
        }
        
        return summary
