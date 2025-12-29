"""Prompty template loader and manager.

This module provides functionality to load and manage Prompty template files
for AI service system prompts.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional
import re


class PromptyTemplate:
    """Represents a loaded Prompty template.
    
    Parses and stores Prompty file content including metadata
    and system prompt.
    """
    
    def __init__(
        self, 
        name: str,
        system_message: str,
        metadata: Dict[str, Any]
    ):
        """Initialize Prompty template.
        
        Args:
            name: Template name
            system_message: System prompt content
            metadata: Template metadata (model, parameters, etc.)
        """
        self.name = name
        self.system_message = system_message
        self.metadata = metadata
        self.logger = logging.getLogger(__name__)
    
    def render(self, context: Dict[str, Any] = None) -> str:
        """Render template with context variables.
        
        Args:
            context: Dictionary of variables to substitute in template
            
        Returns:
            Rendered system message
        """
        if not context:
            return self.system_message
        
        rendered = self.system_message
        
        # Simple variable substitution: {{variable_name}}
        for key, value in context.items():
            pattern = r'\{\{' + re.escape(key) + r'\}\}'
            rendered = re.sub(pattern, str(value), rendered)
        
        return rendered
    
    def get_model_parameters(self) -> Dict[str, Any]:
        """Get model parameters from metadata.
        
        Returns:
            Dictionary of model parameters (max_tokens, temperature, etc.)
        """
        return self.metadata.get('model', {}).get('parameters', {})
    
    def get_max_tokens(self) -> Optional[int]:
        """Get max_tokens parameter.
        
        Returns:
            Max tokens value or None
        """
        return self.get_model_parameters().get('max_tokens')
    
    def get_temperature(self) -> Optional[float]:
        """Get temperature parameter.
        
        Returns:
            Temperature value or None
        """
        return self.get_model_parameters().get('temperature')


class PromptyLoader:
    """Load and manage Prompty template files.
    
    Provides caching and template management for AI service prompts.
    """
    
    def __init__(self, prompts_dir: Path = None):
        """Initialize Prompty loader.
        
        Args:
            prompts_dir: Directory containing .prompty files
        """
        if prompts_dir is None:
            # Default to src/prompts directory
            prompts_dir = Path(__file__).parent.parent / "prompts"
        
        self.prompts_dir = Path(prompts_dir)
        self.cache: Dict[str, PromptyTemplate] = {}
        self.logger = logging.getLogger(__name__)
        
        if not self.prompts_dir.exists():
            self.logger.warning(f"Prompts directory not found: {self.prompts_dir}")
    
    def load_prompt(self, name: str) -> PromptyTemplate:
        """Load Prompty template by name.
        
        Args:
            name: Template name (without .prompty extension)
            
        Returns:
            Loaded PromptyTemplate
            
        Raises:
            FileNotFoundError: If template file not found
            ValueError: If template parsing fails
        """
        # Check cache first
        if name in self.cache:
            self.logger.debug(f"Using cached template: {name}")
            return self.cache[name]
        
        # Load from file
        template_path = self.prompts_dir / f"{name}.prompty"
        
        if not template_path.exists():
            raise FileNotFoundError(
                f"Prompty template not found: {template_path}"
            )
        
        self.logger.info(f"Loading Prompty template: {name}")
        
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            template = self._parse_prompty(name, content)
            
            # Cache the template
            self.cache[name] = template
            
            return template
            
        except Exception as e:
            self.logger.error(f"Error loading template {name}: {e}")
            raise ValueError(f"Failed to parse Prompty template: {e}")
    
    def _parse_prompty(self, name: str, content: str) -> PromptyTemplate:
        """Parse Prompty file content.
        
        Args:
            name: Template name
            content: File content
            
        Returns:
            Parsed PromptyTemplate
        """
        # Split into metadata and system message sections
        parts = content.split('---', 2)
        
        if len(parts) < 3:
            raise ValueError("Invalid Prompty format: missing --- separators")
        
        metadata_section = parts[1].strip()
        body_section = parts[2].strip()
        
        # Parse metadata (simple YAML-like parsing)
        metadata = self._parse_metadata(metadata_section)
        
        # Extract system message
        system_message = self._extract_system_message(body_section)
        
        return PromptyTemplate(
            name=name,
            system_message=system_message,
            metadata=metadata
        )
    
    def _parse_metadata(self, metadata_section: str) -> Dict[str, Any]:
        """Parse metadata section.
        
        Args:
            metadata_section: Metadata YAML content
            
        Returns:
            Metadata dictionary
        """
        metadata = {}
        stack = [metadata]  # Stack for nested structures
        indent_levels = [0]  # Track indentation levels
        
        for line in metadata_section.split('\n'):
            if not line.strip() or line.strip().startswith('#'):
                continue
            
            # Calculate indentation level
            indent = len(line) - len(line.lstrip())
            
            # Pop stack if we've dedented
            while len(indent_levels) > 1 and indent <= indent_levels[-1]:
                indent_levels.pop()
                stack.pop()
            
            current_dict = stack[-1]
            
            # Handle key: value pairs
            if ':' in line:
                key_part, _, value_part = line.partition(':')
                key = key_part.strip()
                value = value_part.strip()
                
                # Nested dictionary
                if not value:
                    new_dict = {}
                    current_dict[key] = new_dict
                    stack.append(new_dict)
                    indent_levels.append(indent)
                else:
                    # Parse value type
                    if value.isdigit():
                        value = int(value)
                    elif value.replace('.', '', 1).replace('-', '', 1).isdigit():
                        value = float(value)
                    elif value.lower() in ('true', 'false'):
                        value = value.lower() == 'true'
                    
                    current_dict[key] = value
            
            # Handle list items
            elif line.strip().startswith('-'):
                item = line.strip()[1:].strip()
                # Find parent key that should hold this list
                if not isinstance(current_dict, list):
                    # Convert last added value to list if needed
                    keys = list(current_dict.keys())
                    if keys:
                        last_key = keys[-1]
                        if not isinstance(current_dict[last_key], list):
                            current_dict[last_key] = [current_dict[last_key]] if current_dict[last_key] else []
                        current_dict[last_key].append(item)
        
        return metadata
    
    def _extract_system_message(self, body_section: str) -> str:
        """Extract system message from body section.
        
        Args:
            body_section: Body content after metadata
            
        Returns:
            System message content
        """
        # Look for "system:" marker
        if 'system:' in body_section:
            parts = body_section.split('system:', 1)
            return parts[1].strip()
        
        # If no marker, return entire body
        return body_section.strip()
    
    def list_available_templates(self) -> list[str]:
        """List all available Prompty templates.
        
        Returns:
            List of template names (without .prompty extension)
        """
        if not self.prompts_dir.exists():
            return []
        
        templates = []
        for file_path in self.prompts_dir.glob("*.prompty"):
            templates.append(file_path.stem)
        
        return sorted(templates)
    
    def clear_cache(self):
        """Clear template cache."""
        self.cache.clear()
        self.logger.info("Cleared Prompty template cache")
    
    def reload_template(self, name: str) -> PromptyTemplate:
        """Reload template from disk, bypassing cache.
        
        Args:
            name: Template name to reload
            
        Returns:
            Reloaded PromptyTemplate
        """
        # Remove from cache
        if name in self.cache:
            del self.cache[name]
        
        # Load fresh copy
        return self.load_prompt(name)
