"""
Analysis Status Tracker and User Feedback System
Provides real-time status updates and progress tracking for all analysis phases
"""

import logging
import sys
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import threading

class AnalysisPhase(Enum):
    """Analysis phases for tracking"""
    INITIALIZATION = "initialization"
    CONNECTION_TEST = "connection_test"
    PERFMON_SETUP = "perfmon_setup"
    PERFMON_COLLECTION = "perfmon_collection"
    SQL_ANALYSIS = "sql_analysis"
    AI_ANALYSIS = "ai_analysis"
    AI_DIALOG = "ai_dialog"
    REPORT_GENERATION = "report_generation"
    CLEANUP = "cleanup"
    COMPLETED = "completed"

class StatusLevel(Enum):
    """Status message levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"
    PROGRESS = "progress"

@dataclass
class PhaseStatus:
    """Status information for an analysis phase"""
    phase: AnalysisPhase
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"  # running, completed, failed, skipped
    progress_percent: float = 0.0
    current_step: str = ""
    step_count: int = 0
    completed_steps: int = 0
    messages: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def duration(self) -> timedelta:
        end = self.end_time or datetime.now()
        return end - self.start_time
    
    @property
    def is_running(self) -> bool:
        return self.status == "running"
    
    @property
    def is_completed(self) -> bool:
        return self.status in ["completed", "failed", "skipped"]

class AnalysisStatusTracker:
    """Tracks and displays analysis progress with real-time CMD updates"""
    
    def __init__(self, config=None):
        """Initialize status tracker
        
        Args:
            config: Configuration manager (optional)
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Status tracking
        self.analysis_start_time = datetime.now()
        self.current_phase: Optional[AnalysisPhase] = None
        self.phase_history: Dict[AnalysisPhase, PhaseStatus] = {}
        self.overall_progress = 0.0
        
        # Display settings
        self.show_real_time_updates = True
        self.show_progress_bar = True
        self.console_width = 80
        self.update_interval = 1.0  # seconds
        
        # Threading for real-time updates
        self.update_thread = None
        self.stop_updates = threading.Event()
        self.last_display_time = datetime.now()
        
        # Phase weights for overall progress calculation
        self.phase_weights = {
            AnalysisPhase.INITIALIZATION: 5,
            AnalysisPhase.CONNECTION_TEST: 5,
            AnalysisPhase.PERFMON_SETUP: 10,
            AnalysisPhase.PERFMON_COLLECTION: 15,
            AnalysisPhase.SQL_ANALYSIS: 30,
            AnalysisPhase.AI_ANALYSIS: 20,
            AnalysisPhase.AI_DIALOG: 10,
            AnalysisPhase.REPORT_GENERATION: 15,
            AnalysisPhase.CLEANUP: 5,
            AnalysisPhase.COMPLETED: 0
        }
    
    def start_analysis(self, server_name: str, analysis_type: str = "standard"):
        """Start analysis tracking
        
        Args:
            server_name: SQL Server name being analyzed
            analysis_type: Type of analysis (standard, ai, perfmon, etc.)
        """
        self.analysis_start_time = datetime.now()
        
        # Print header
        self._print_header(server_name, analysis_type)
        
        # Start real-time updates
        if self.show_real_time_updates:
            self._start_update_thread()
    
    def start_phase(self, phase: AnalysisPhase, step_count: int = 1, description: Optional[str] = None) -> PhaseStatus:
        """Start a new analysis phase
        
        Args:
            phase: Analysis phase to start
            step_count: Total number of steps in this phase
            description: Optional phase description
            
        Returns:
            PhaseStatus object for the started phase
        """
        # Complete previous phase if running
        if self.current_phase and self.current_phase in self.phase_history:
            current_status = self.phase_history[self.current_phase]
            if current_status.is_running:
                self.complete_phase(self.current_phase)
        
        # Start new phase
        self.current_phase = phase
        phase_status = PhaseStatus(
            phase=phase,
            start_time=datetime.now(),
            step_count=step_count,
            current_step=description or f"Starting {phase.value}..."
        )
        
        self.phase_history[phase] = phase_status
        
        self._log_status(f"🚀 Starting: {phase.value.replace('_', ' ').title()}", StatusLevel.INFO)
        if description:
            self._log_status(f"   {description}", StatusLevel.INFO)
        
        self._update_overall_progress()
        return phase_status
    
    def update_phase_progress(self, phase: AnalysisPhase, completed_steps: Optional[int] = None, 
                            current_step: Optional[str] = None, progress_percent: Optional[float] = None):
        """Update progress for current phase
        
        Args:
            phase: Phase to update
            completed_steps: Number of completed steps
            current_step: Description of current step
            progress_percent: Progress percentage (0-100)
        """
        if phase not in self.phase_history:
            return
        
        phase_status = self.phase_history[phase]
        
        if completed_steps is not None:
            phase_status.completed_steps = min(completed_steps, phase_status.step_count)
            
        if current_step is not None:
            phase_status.current_step = current_step
            
        if progress_percent is not None:
            phase_status.progress_percent = min(progress_percent, 100.0)
        elif phase_status.step_count > 0:
            phase_status.progress_percent = (phase_status.completed_steps / phase_status.step_count) * 100
        
        self._update_overall_progress()
    
    def complete_phase(self, phase: AnalysisPhase, status: str = "completed", message: Optional[str] = None):
        """Complete an analysis phase
        
        Args:
            phase: Phase to complete
            status: Completion status (completed, failed, skipped)
            message: Optional completion message
        """
        if phase not in self.phase_history:
            return
        
        phase_status = self.phase_history[phase]
        phase_status.end_time = datetime.now()
        phase_status.status = status
        phase_status.progress_percent = 100.0 if status == "completed" else phase_status.progress_percent
        phase_status.completed_steps = phase_status.step_count if status == "completed" else phase_status.completed_steps
        
        # Log completion
        if status == "completed":
            emoji = "✅"
            level = StatusLevel.SUCCESS
        elif status == "failed":
            emoji = "❌"
            level = StatusLevel.ERROR
        elif status == "skipped":
            emoji = "⏭️"
            level = StatusLevel.WARNING
        else:
            emoji = "🏁"
            level = StatusLevel.INFO
        
        duration_str = f"({phase_status.duration.total_seconds():.1f}s)"
        status_message = f"{emoji} {phase.value.replace('_', ' ').title()}: {status} {duration_str}"
        
        if message:
            status_message += f" - {message}"
        
        self._log_status(status_message, level)
        self._update_overall_progress()
    
    def log_message(self, message: str, level: StatusLevel = StatusLevel.INFO, phase: Optional[AnalysisPhase] = None):
        """Log a status message
        
        Args:
            message: Message to log
            level: Message level
            phase: Associated phase (optional)
        """
        target_phase = phase or self.current_phase
        
        if target_phase and target_phase in self.phase_history:
            self.phase_history[target_phase].messages.append({
                'timestamp': datetime.now(),
                'message': message,
                'level': level.value
            })
        
        self._log_status(message, level)
    
    def complete_analysis(self, success: bool = True, summary: Optional[Dict[str, Any]] = None):
        """Complete the entire analysis
        
        Args:
            success: Whether analysis completed successfully
            summary: Analysis summary data
        """
        # Complete current phase
        if self.current_phase and self.current_phase in self.phase_history:
            current_status = self.phase_history[self.current_phase]
            if current_status.is_running:
                self.complete_phase(self.current_phase)
        
        # Stop updates
        self._stop_update_thread()
        
        # Print final summary
        self._print_summary(success, summary)
    
    def _start_update_thread(self):
        """Start real-time update thread"""
        if self.update_thread and self.update_thread.is_alive():
            return
        
        self.stop_updates.clear()
        self.update_thread = threading.Thread(
            target=self._update_loop,
            name="StatusUpdateThread",
            daemon=True
        )
        self.update_thread.start()
    
    def _stop_update_thread(self):
        """Stop real-time update thread"""
        if not self.update_thread:
            return
        
        self.stop_updates.set()
        if self.update_thread.is_alive():
            self.update_thread.join(timeout=2)
    
    def _update_loop(self):
        """Real-time update loop"""
        while not self.stop_updates.is_set():
            try:
                if datetime.now() - self.last_display_time >= timedelta(seconds=self.update_interval):
                    self._refresh_display()
                    self.last_display_time = datetime.now()
                
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Error in status update loop: {e}")
                time.sleep(1)
    
    def _refresh_display(self):
        """Refresh the console display"""
        if not self.current_phase or self.current_phase not in self.phase_history:
            return
        
        current_status = self.phase_history[self.current_phase]
        
        if not current_status.is_running:
            return
        
        # Show current step and progress
        if self.show_progress_bar and current_status.progress_percent > 0:
            progress_bar = self._create_progress_bar(current_status.progress_percent)
            step_info = f"   {current_status.current_step}"
            
            # Print with carriage return to overwrite
            print(f"\r{progress_bar} {step_info}", end="", flush=True)
    
    def _create_progress_bar(self, percentage: float, width: int = 30) -> str:
        """Create ASCII progress bar
        
        Args:
            percentage: Progress percentage (0-100)
            width: Width of progress bar
            
        Returns:
            ASCII progress bar string
        """
        filled = int(width * percentage / 100)
        bar = "█" * filled + "░" * (width - filled)
        return f"[{bar}] {percentage:5.1f}%"
    
    def _update_overall_progress(self):
        """Update overall analysis progress"""
        total_weight = sum(self.phase_weights.values())
        completed_weight = 0
        
        for phase, weight in self.phase_weights.items():
            if phase in self.phase_history:
                status = self.phase_history[phase]
                if status.is_completed:
                    completed_weight += weight
                elif status.is_running:
                    completed_weight += weight * (status.progress_percent / 100)
        
        self.overall_progress = (completed_weight / total_weight) * 100 if total_weight > 0 else 0
    
    def _print_header(self, server_name: str, analysis_type: str):
        """Print analysis header"""
        print("\n" + "="*self.console_width)
        print("⚡ SQL SPEEDINATOR - PERFORMANCE ANALYSIS".center(self.console_width))
        print("="*self.console_width)
        print(f"🗄️  Server: {server_name}")
        print(f"📊 Analysis Type: {analysis_type.upper()}")
        print(f"🕒 Started: {self.analysis_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*self.console_width)
        print()
    
    def _print_summary(self, success: bool, summary: Optional[Dict[str, Any]]):
        """Print final analysis summary"""
        print("\n" + "="*self.console_width)
        
        if success:
            print("✅ ANALYSIS COMPLETED SUCCESSFULLY".center(self.console_width))
        else:
            print("❌ ANALYSIS COMPLETED WITH ISSUES".center(self.console_width))
        
        print("="*self.console_width)
        
        # Duration
        total_duration = datetime.now() - self.analysis_start_time
        print(f"⏱️  Total Duration: {total_duration.total_seconds():.1f} seconds")
        
        # Phase summary
        print(f"📊 Phase Summary:")
        for phase, status in self.phase_history.items():
            emoji = "✅" if status.status == "completed" else ("❌" if status.status == "failed" else "⏭️")
            duration = status.duration.total_seconds()
            print(f"   {emoji} {phase.value.replace('_', ' ').title()}: {status.status} ({duration:.1f}s)")
        
        # Overall progress
        print(f"📈 Overall Progress: {self.overall_progress:.1f}%")
        
        # Summary data
        if summary:
            print("\n📋 Analysis Results:")
            for key, value in summary.items():
                if isinstance(value, (int, float)):
                    print(f"   • {key.replace('_', ' ').title()}: {value}")
                elif isinstance(value, str) and len(value) < 100:
                    print(f"   • {key.replace('_', ' ').title()}: {value}")
        
        print("="*self.console_width)
        print()
    
    def _log_status(self, message: str, level: StatusLevel):
        """Log status message to console
        
        Args:
            message: Message to log
            level: Message level
        """
        # Level indicators
        level_indicators = {
            StatusLevel.INFO: "ℹ️",
            StatusLevel.WARNING: "⚠️",
            StatusLevel.ERROR: "❌",
            StatusLevel.SUCCESS: "✅",
            StatusLevel.PROGRESS: "🔄"
        }
        
        indicator = level_indicators.get(level, "•")
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Print with timestamp
        print(f"[{timestamp}] {indicator} {message}")
        
        # Also log to logger if available
        if hasattr(self, 'logger'):
            if level == StatusLevel.ERROR:
                self.logger.error(message)
            elif level == StatusLevel.WARNING:
                self.logger.warning(message)
            else:
                self.logger.info(message)
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get comprehensive status summary
        
        Returns:
            Dictionary with current status information
        """
        total_duration = datetime.now() - self.analysis_start_time
        
        phase_summaries = {}
        for phase, status in self.phase_history.items():
            phase_summaries[phase.value] = {
                'status': status.status,
                'progress_percent': status.progress_percent,
                'duration_seconds': status.duration.total_seconds(),
                'current_step': status.current_step,
                'completed_steps': status.completed_steps,
                'total_steps': status.step_count,
                'message_count': len(status.messages)
            }
        
        return {
            'overall_progress': self.overall_progress,
            'total_duration_seconds': total_duration.total_seconds(),
            'current_phase': self.current_phase.value if self.current_phase else None,
            'analysis_start_time': self.analysis_start_time.isoformat(),
            'phases': phase_summaries,
            'is_running': any(status.is_running for status in self.phase_history.values())
        }