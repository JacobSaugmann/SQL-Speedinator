"""
File Cleanup Manager
Automatically cleans up old logs, perfmon data, and reports to prevent disk space issues
"""

import logging
import os
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

class FileCleanupManager:
    """Manages automatic cleanup of old files to prevent disk space issues"""
    
    def __init__(self, config=None):
        """Initialize file cleanup manager
        
        Args:
            config: ConfigManager instance (optional)
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Default cleanup settings
        self.cleanup_settings = {
            'enabled': True,
            'retention_days': 5,  # Keep files for 5 days
            'dry_run': False,     # Set to True to see what would be deleted without actually deleting
            'skip_active_files': True,  # Skip files that are currently being written to
            'max_cleanup_time_seconds': 30  # Maximum time to spend on cleanup
        }
        
        # Define file patterns to clean up
        self.cleanup_patterns = {
            'logs': {
                'patterns': ['*.log', '*.log.*'],
                'directories': ['logs', 'src/logs'],
                'description': 'Log files'
            },
            'perfmon_data': {
                'patterns': ['*.json', '*.blg', '*.csv', '*.xml', '*.ps1'],
                'directories': ['src/perfmon/data', 'src/perfmon/templates'],
                'description': 'Performance Monitor data files'
            },
            'reports': {
                'patterns': ['*.pdf', '*.html', '*.json'],
                'directories': ['reports', 'output'],
                'description': 'Generated reports'
            },
            'temp_files': {
                'patterns': ['*.tmp', '*.temp', '*~'],
                'directories': ['.', 'src', 'temp'],
                'description': 'Temporary files'
            }
        }
        
        # Get base directory (project root)
        self.base_dir = Path(__file__).parent.parent.parent
        
    def cleanup_old_files(self) -> Dict[str, Any]:
        """Clean up old files based on retention policy
        
        Returns:
            Dictionary with cleanup results
        """
        if not self.cleanup_settings['enabled']:
            self.logger.info("File cleanup is disabled")
            return {'enabled': False}
        
        start_time = time.time()
        cleanup_results = {
            'enabled': True,
            'start_time': datetime.now().isoformat(),
            'categories': {},
            'total_files_deleted': 0,
            'total_space_freed_mb': 0,
            'errors': [],
            'skipped_files': []
        }
        
        try:
            cutoff_date = datetime.now() - timedelta(days=self.cleanup_settings['retention_days'])
            self.logger.info(f"Starting file cleanup (keeping files newer than {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')})")
            
            for category, settings in self.cleanup_patterns.items():
                category_result = self._cleanup_category(category, settings, cutoff_date)
                cleanup_results['categories'][category] = category_result
                cleanup_results['total_files_deleted'] += category_result['files_deleted']
                cleanup_results['total_space_freed_mb'] += category_result['space_freed_mb']
                cleanup_results['errors'].extend(category_result['errors'])
                cleanup_results['skipped_files'].extend(category_result['skipped_files'])
                
                # Check if we're taking too long
                elapsed_time = time.time() - start_time
                if elapsed_time > self.cleanup_settings['max_cleanup_time_seconds']:
                    self.logger.warning(f"Cleanup timeout reached ({elapsed_time:.1f}s), stopping")
                    break
            
            cleanup_results['end_time'] = datetime.now().isoformat()
            cleanup_results['duration_seconds'] = time.time() - start_time
            
            # Log summary
            self._log_cleanup_summary(cleanup_results)
            
            return cleanup_results
            
        except Exception as e:
            self.logger.error(f"Error during file cleanup: {e}")
            cleanup_results['errors'].append(f"General cleanup error: {str(e)}")
            return cleanup_results
    
    def _cleanup_category(self, category: str, settings: Dict[str, Any], cutoff_date: datetime) -> Dict[str, Any]:
        """Clean up files in a specific category
        
        Args:
            category: Category name (e.g., 'logs', 'perfmon_data')
            settings: Category settings with patterns and directories
            cutoff_date: Files older than this date will be deleted
            
        Returns:
            Dictionary with category cleanup results
        """
        result = {
            'category': category,
            'files_deleted': 0,
            'space_freed_mb': 0,
            'files_found': 0,
            'errors': [],
            'skipped_files': []
        }
        
        try:
            self.logger.info(f"Cleaning up {settings['description']}...")
            
            for directory in settings['directories']:
                dir_path = self.base_dir / directory
                if not dir_path.exists():
                    continue
                
                for pattern in settings['patterns']:
                    files_found = list(dir_path.rglob(pattern))
                    result['files_found'] += len(files_found)
                    
                    for file_path in files_found:
                        try:
                            if self._should_delete_file(file_path, cutoff_date):
                                file_size_mb = self._get_file_size_mb(file_path)
                                
                                if self.cleanup_settings['dry_run']:
                                    self.logger.info(f"[DRY RUN] Would delete: {file_path}")
                                    result['files_deleted'] += 1
                                    result['space_freed_mb'] += file_size_mb
                                else:
                                    file_path.unlink()
                                    result['files_deleted'] += 1
                                    result['space_freed_mb'] += file_size_mb
                                    self.logger.debug(f"Deleted: {file_path} ({file_size_mb:.2f} MB)")
                            else:
                                result['skipped_files'].append(str(file_path))
                                
                        except Exception as e:
                            error_msg = f"Error deleting {file_path}: {str(e)}"
                            result['errors'].append(error_msg)
                            self.logger.warning(error_msg)
            
            if result['files_deleted'] > 0:
                action = "Would delete" if self.cleanup_settings['dry_run'] else "Deleted"
                self.logger.info(f"[COMPLETED] {action} {result['files_deleted']} {settings['description'].lower()} "
                                 f"({result['space_freed_mb']:.2f} MB freed)")
                                 
        except Exception as e:
            error_msg = f"Error cleaning category {category}: {str(e)}"
            result['errors'].append(error_msg)
            self.logger.error(error_msg)
        
        return result
    
    def _should_delete_file(self, file_path: Path, cutoff_date: datetime) -> bool:
        """Determine if a file should be deleted
        
        Args:
            file_path: Path to the file
            cutoff_date: Files older than this will be deleted
            
        Returns:
            True if file should be deleted, False otherwise
        """
        try:
            # Get file modification time
            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            
            # Check if file is older than cutoff
            if file_mtime >= cutoff_date:
                return False
            
            # Additional safety checks
            if self.cleanup_settings['skip_active_files']:
                # Check if file is currently being written to (Windows)
                if self._is_file_active(file_path):
                    return False
            
            # Don't delete files that are too large (safety check)
            file_size_mb = self._get_file_size_mb(file_path)
            if file_size_mb > 1000:  # Don't auto-delete files larger than 1GB
                self.logger.warning(f"Skipping large file: {file_path} ({file_size_mb:.1f} MB)")
                return False
            
            return True
            
        except Exception as e:
            self.logger.debug(f"Error checking file {file_path}: {e}")
            return False
    
    def _is_file_active(self, file_path: Path) -> bool:
        """Check if a file is currently being written to
        
        Args:
            file_path: Path to check
            
        Returns:
            True if file appears to be active, False otherwise
        """
        try:
            # Try to open file in exclusive mode to see if it's locked
            with open(file_path, 'r+b') as f:
                pass
            return False
        except (PermissionError, IOError):
            # File is locked/active
            return True
        except Exception:
            # Other error, assume file is safe to delete
            return False
    
    def _get_file_size_mb(self, file_path: Path) -> float:
        """Get file size in megabytes
        
        Args:
            file_path: Path to file
            
        Returns:
            File size in MB
        """
        try:
            return file_path.stat().st_size / (1024 * 1024)
        except Exception:
            return 0.0
    
    def _log_cleanup_summary(self, results: Dict[str, Any]):
        """Log cleanup summary to console and logger
        
        Args:
            results: Cleanup results dictionary
        """
        if results['total_files_deleted'] == 0:
            self.logger.info("File cleanup completed - no old files found to delete")
            return
        
        action = "Would delete" if self.cleanup_settings['dry_run'] else "Deleted"
        dry_run_text = " [DRY RUN]" if self.cleanup_settings['dry_run'] else ""
        
        summary_lines = [
            f"File cleanup completed{dry_run_text}:",
            f"   Files {action.lower()}: {results['total_files_deleted']:,}",
            f"   Space freed: {results['total_space_freed_mb']:.2f} MB",
            f"   Duration: {results['duration_seconds']:.2f} seconds"
        ]
        
        # Add category breakdown
        for category, category_result in results['categories'].items():
            if category_result['files_deleted'] > 0:
                summary_lines.append(
                    f"   • {category.replace('_', ' ').title()}: "
                    f"{category_result['files_deleted']} files "
                    f"({category_result['space_freed_mb']:.2f} MB)"
                )
        
        # Add errors if any
        if results['errors']:
            summary_lines.append(f"   ⚠️  Errors: {len(results['errors'])}")
        
        for line in summary_lines:
            self.logger.info(line)
    
    def cleanup_specific_directory(self, directory: str, pattern: str = "*", age_days: Optional[int] = None) -> int:
        """Clean up files in a specific directory
        
        Args:
            directory: Directory path relative to project root
            pattern: File pattern to match (default: all files)
            age_days: Override default retention days
            
        Returns:
            Number of files deleted
        """
        if age_days is None:
            age_days = self.cleanup_settings['retention_days']
        
        cutoff_date = datetime.now() - timedelta(days=age_days)
        dir_path = self.base_dir / directory
        
        if not dir_path.exists():
            return 0
        
        files_deleted = 0
        
        try:
            for file_path in dir_path.rglob(pattern):
                if file_path.is_file() and self._should_delete_file(file_path, cutoff_date):
                    if not self.cleanup_settings['dry_run']:
                        file_path.unlink()
                    files_deleted += 1
                    self.logger.debug(f"Deleted: {file_path}")
            
            if files_deleted > 0:
                action = "Would delete" if self.cleanup_settings['dry_run'] else "Deleted"
                self.logger.info(f"{action} {files_deleted} files from {directory}")
        
        except Exception as e:
            self.logger.error(f"Error cleaning directory {directory}: {e}")
        
        return files_deleted
    
    def get_cleanup_preview(self) -> Dict[str, Any]:
        """Get preview of what would be cleaned up without actually deleting
        
        Returns:
            Dictionary with preview results
        """
        original_dry_run = self.cleanup_settings['dry_run']
        self.cleanup_settings['dry_run'] = True
        
        try:
            results = self.cleanup_old_files()
            return results
        finally:
            self.cleanup_settings['dry_run'] = original_dry_run
    
    def configure_cleanup(self, **kwargs):
        """Configure cleanup settings
        
        Args:
            **kwargs: Settings to update (retention_days, enabled, dry_run, etc.)
        """
        for key, value in kwargs.items():
            if key in self.cleanup_settings:
                self.cleanup_settings[key] = value
                self.logger.info(f"Updated cleanup setting: {key} = {value}")
            else:
                self.logger.warning(f"Unknown cleanup setting: {key}")
    
    def add_cleanup_pattern(self, category: str, patterns: List[str], directories: List[str], description: str):
        """Add new file patterns to cleanup
        
        Args:
            category: Category name
            patterns: List of file patterns to match
            directories: List of directories to search
            description: Description for logging
        """
        self.cleanup_patterns[category] = {
            'patterns': patterns,
            'directories': directories,
            'description': description
        }
        self.logger.info(f"Added cleanup pattern: {category}")