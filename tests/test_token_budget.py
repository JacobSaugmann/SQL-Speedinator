"""
Unit tests for Token Budget and Prompty Integration
Tests TokenCounter, TokenBudget, and PromptyLoader functionality
"""

import pytest
import json
from pathlib import Path
from src.services.token_counter import TokenCounter
from src.services.token_budget import TokenBudget, DataPriority
from src.services.prompty_loader import PromptyLoader, PromptyTemplate


class TestTokenCounter:
    """Test TokenCounter functionality"""
    
    def test_count_simple_text(self):
        """Test counting tokens in simple text"""
        counter = TokenCounter()
        text = "Hello world, this is a test."
        token_count = counter.count_tokens(text)
        
        assert token_count > 0
        assert isinstance(token_count, int)
        # Rough estimate: should be around 7-8 tokens
        assert 5 <= token_count <= 15
    
    def test_count_empty_text(self):
        """Test counting tokens in empty string"""
        counter = TokenCounter()
        assert counter.count_tokens("") == 0
        assert counter.count_tokens(None) == 0
    
    def test_count_messages_tokens(self):
        """Test counting tokens in chat messages"""
        counter = TokenCounter()
        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "What is the weather?"}
        ]
        
        token_count = counter.count_messages_tokens(messages)
        assert token_count > 0
        # Should include message formatting overhead
        assert token_count > counter.count_tokens("You are a helpful assistant.What is the weather?")
    
    def test_estimate_response_tokens(self):
        """Test response token estimation"""
        counter = TokenCounter()
        prompt_tokens = 100
        
        estimated = counter.estimate_response_tokens(prompt_tokens)
        assert estimated > 0
        assert isinstance(estimated, int)
        # Should be reasonable proportion of prompt
        assert estimated >= 50  # At least 50% of prompt or minimum 100
    
    def test_fits_in_budget(self):
        """Test budget checking"""
        counter = TokenCounter()
        
        # Small text should fit
        fits, count = counter.fits_in_budget("Hello world", budget=1000)
        assert fits is True
        assert count > 0
        
        # Large text should not fit in tiny budget
        large_text = " ".join(["word"] * 1000)
        fits, count = counter.fits_in_budget(large_text, budget=50, include_response_estimate=False)
        assert fits is False
        assert count > 50
    
    def test_get_budget_remaining(self):
        """Test budget remaining calculation"""
        counter = TokenCounter()
        
        remaining, percentage = counter.get_budget_remaining(used_tokens=250, total_budget=1000)
        assert remaining == 750
        assert percentage == 25.0
        
        # Test overflow protection
        remaining, percentage = counter.get_budget_remaining(used_tokens=1500, total_budget=1000)
        assert remaining == 0
        assert percentage == 150.0


class TestTokenBudget:
    """Test TokenBudget functionality"""
    
    def test_check_budget_within_limit(self):
        """Test budget check for data within limits"""
        budget = TokenBudget()
        
        small_data = {
            "server_info": {"cpu": 8, "memory": 64000},
            "wait_stats": {"top_waits": [{"type": "PAGEIOLATCH", "pct": 15.5}]}
        }
        
        fits, token_count, reason = budget.check_budget(small_data)
        assert fits is True
        assert token_count > 0
        assert "Within budget" in reason
    
    def test_check_budget_over_limit(self):
        """Test budget check for data exceeding limits"""
        budget = TokenBudget()
        
        # Create large data that exceeds budget
        large_data = {
            "massive_list": [{"item": i, "data": "x" * 100} for i in range(1000)]
        }
        
        fits, token_count, reason = budget.check_budget(large_data)
        assert fits is False
        assert "Over budget" in reason
    
    def test_reduce_to_budget(self):
        """Test data reduction to fit budget"""
        budget = TokenBudget()
        
        # Create actually LARGE data that will need reduction
        large_data = {
            "wait_stats": {"waits": [{"type": f"WAIT_{i}", "pct": i} for i in range(100)]},  # CRITICAL
            "disk_issues": {"issues": [{"disk": f"Drive_{i}", "latency": i} for i in range(100)]},  # CRITICAL
            "index_issues": {"indexes": [{"table": f"Table_{i}", "frag": i} for i in range(100)]},  # HIGH
            "database_info": {"databases": [{"name": f"DB_{i}", "size": i * 1000} for i in range(200)]}  # LOW
        }
        
        # Verify it's over budget first
        fits, original_tokens, _ = budget.check_budget(large_data)
        assert not fits, "Test data should exceed budget"
        
        # Reduce to small budget
        reduced = budget.reduce_to_budget(large_data, target_tokens=500)
        
        # Critical data should be preserved
        assert "wait_stats" in reduced or "disk_issues" in reduced
        
        # Result should be reduced in size
        reduced_tokens = budget._get_token_count(reduced)
        assert reduced_tokens < original_tokens, "Data should be reduced"
    
    def test_priority_extraction(self):
        """Test extracting sections by priority"""
        budget = TokenBudget()
        
        data = {
            "wait_stats": "critical_data",
            "index_issues": "high_priority",
            "database_info": "low_priority"
        }
        
        critical = budget._extract_priority_sections(data, DataPriority.CRITICAL)
        assert "wait_stats" in critical
        assert "index_issues" not in critical
        
        high = budget._extract_priority_sections(data, DataPriority.HIGH)
        assert "index_issues" in high
        assert "wait_stats" not in high
    
    def test_get_budget_summary(self):
        """Test budget summary generation"""
        budget = TokenBudget()
        
        summary = budget.get_budget_summary(
            system_prompt_tokens=300,
            data_tokens=1500,
            response_tokens=800
        )
        
        assert summary['within_budget'] in [True, False]
        assert summary['total']['used'] == 2600
        assert summary['system_prompt']['used'] == 300
        assert summary['performance_data']['used'] == 1500
        assert 'percentage' in summary['total']


class TestPromptyLoader:
    """Test PromptyLoader functionality"""
    
    @pytest.fixture
    def prompts_dir(self, tmp_path):
        """Create temporary prompts directory with test templates"""
        prompts_dir = tmp_path / "prompts"
        prompts_dir.mkdir()
        
        # Create test template
        test_template = prompts_dir / "test_template.prompty"
        test_template.write_text("""---
name: Test Template
description: Test prompty template
authors:
  - Test Author
model:
  api: chat
  configuration:
    type: azure_openai
  parameters:
    max_tokens: 1000
    temperature: 0.3
version: 1.0.0
---

system:
You are a test assistant. This is a {{variable}} test.
Provide helpful responses.""")
        
        return prompts_dir
    
    def test_load_prompt_success(self, prompts_dir):
        """Test successful prompt loading"""
        loader = PromptyLoader(prompts_dir)
        
        template = loader.load_prompt("test_template")
        
        assert isinstance(template, PromptyTemplate)
        assert template.name == "test_template"
        assert "test assistant" in template.system_message.lower()
        assert template.metadata['name'] == 'Test Template'
    
    def test_load_prompt_not_found(self, prompts_dir):
        """Test loading non-existent prompt"""
        loader = PromptyLoader(prompts_dir)
        
        with pytest.raises(FileNotFoundError):
            loader.load_prompt("nonexistent")
    
    def test_prompt_caching(self, prompts_dir):
        """Test that prompts are cached"""
        loader = PromptyLoader(prompts_dir)
        
        # Load twice
        template1 = loader.load_prompt("test_template")
        template2 = loader.load_prompt("test_template")
        
        # Should be same instance (cached)
        assert template1 is template2
        assert "test_template" in loader.cache
    
    def test_template_rendering(self, prompts_dir):
        """Test template variable rendering"""
        loader = PromptyLoader(prompts_dir)
        template = loader.load_prompt("test_template")
        
        # Render with context
        rendered = template.render({"variable": "AWESOME"})
        assert "AWESOME" in rendered
        assert "{{variable}}" not in rendered
    
    def test_get_model_parameters(self, prompts_dir):
        """Test retrieving model parameters"""
        loader = PromptyLoader(prompts_dir)
        template = loader.load_prompt("test_template")
        
        params = template.get_model_parameters()
        assert params['max_tokens'] == 1000
        assert params['temperature'] == 0.3
        
        assert template.get_max_tokens() == 1000
        assert template.get_temperature() == 0.3
    
    def test_list_available_templates(self, prompts_dir):
        """Test listing available templates"""
        loader = PromptyLoader(prompts_dir)
        
        templates = loader.list_available_templates()
        assert "test_template" in templates
    
    def test_clear_cache(self, prompts_dir):
        """Test cache clearing"""
        loader = PromptyLoader(prompts_dir)
        
        loader.load_prompt("test_template")
        assert len(loader.cache) > 0
        
        loader.clear_cache()
        assert len(loader.cache) == 0
    
    def test_reload_template(self, prompts_dir):
        """Test reloading template from disk"""
        loader = PromptyLoader(prompts_dir)
        
        # Load and cache
        template1 = loader.load_prompt("test_template")
        
        # Reload
        template2 = loader.reload_template("test_template")
        
        # Should be different instances
        assert template1 is not template2
        assert template1.name == template2.name


class TestPromptyIntegration:
    """Integration tests for Prompty with actual project templates"""
    
    def test_load_sql_performance_specialist(self):
        """Test loading actual SQL performance specialist template"""
        prompts_dir = Path(__file__).parent.parent / "src" / "prompts"
        
        if not prompts_dir.exists():
            pytest.skip("Prompts directory not found")
        
        loader = PromptyLoader(prompts_dir)
        
        try:
            template = loader.load_prompt("sql_performance_specialist")
            
            assert template.name == "sql_performance_specialist"
            assert "SQL Server" in template.system_message
            assert "bottleneck" in template.system_message.lower()
            
            # Check model parameters
            assert template.get_max_tokens() is not None
            assert template.get_temperature() is not None
            
        except FileNotFoundError:
            pytest.skip("SQL performance specialist template not created yet")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
