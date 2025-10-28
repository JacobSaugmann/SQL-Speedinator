"""
AI Dialog System for SQL Server Performance Analysis
Provides intelligent multi-turn conversations for bottleneck identification
"""

import logging
import json
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

@dataclass
class DialogContext:
    """Context for AI dialog session"""
    session_id: str
    start_time: datetime
    total_tokens_used: int
    max_tokens_allowed: int
    max_turns: int
    current_turn: int
    conversation_history: List[Dict[str, Any]]
    local_analysis_results: Dict[str, Any]
    identified_bottlenecks: List[Dict[str, Any]]
    confidence_score: float
    status: str  # 'active', 'completed', 'aborted', 'token_limit_reached'

class AIDialogSystem:
    """Multi-turn AI conversation system for performance analysis"""
    
    def __init__(self, ai_service, config):
        """Initialize AI dialog system
        
        Args:
            ai_service: AIService instance
            config: ConfigManager instance
        """
        self.ai_service = ai_service
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Dialog configuration
        self.max_tokens_per_session = getattr(config, 'ai_max_tokens_per_session', 5000)
        self.max_turns_per_session = getattr(config, 'ai_max_turns_per_session', 8)
        self.confidence_threshold = getattr(config, 'ai_confidence_threshold', 0.8)
        self.max_questions_per_turn = 3
        
    def start_bottleneck_investigation(self, analysis_results: Dict[str, Any]) -> DialogContext:
        """Start AI-driven bottleneck investigation dialog
        
        Args:
            analysis_results: Complete analysis results from SQL Server and PerfMon
            
        Returns:
            DialogContext with investigation results
        """
        session_id = f"dialog_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.logger.info(f"🤖 Starting AI bottleneck investigation session: {session_id}")
        
        # Initialize dialog context
        context = DialogContext(
            session_id=session_id,
            start_time=datetime.now(),
            total_tokens_used=0,
            max_tokens_allowed=self.max_tokens_per_session,
            max_turns=self.max_turns_per_session,
            current_turn=0,
            conversation_history=[],
            local_analysis_results=analysis_results,
            identified_bottlenecks=[],
            confidence_score=0.0,
            status='active'
        )
        
        # First, run local bottleneck detection
        local_bottlenecks = self._run_local_bottleneck_analysis(analysis_results)
        
        if local_bottlenecks['confidence'] >= self.confidence_threshold:
            self.logger.info(f"✅ Local analysis achieved high confidence ({local_bottlenecks['confidence']:.1%})")
            context.identified_bottlenecks = local_bottlenecks['bottlenecks']
            context.confidence_score = local_bottlenecks['confidence']
            context.status = 'completed'
            return context
        
        self.logger.info(f"🔍 Local analysis confidence low ({local_bottlenecks['confidence']:.1%}), starting AI dialog...")
        
        # Start AI dialog for unclear cases
        context = self._conduct_ai_dialog(context, local_bottlenecks)
        
        return context
    
    def _run_local_bottleneck_analysis(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Run local (non-AI) bottleneck detection logic
        
        Args:
            analysis_results: Analysis data
            
        Returns:
            Dict with bottlenecks and confidence score
        """
        self.logger.info("🔧 Running local bottleneck analysis...")
        
        bottlenecks = []
        confidence_factors = []
        
        # CPU Analysis
        cpu_bottleneck = self._analyze_cpu_bottleneck(analysis_results)
        if cpu_bottleneck:
            bottlenecks.append(cpu_bottleneck)
            confidence_factors.append(cpu_bottleneck['confidence'])
        
        # Memory Analysis
        memory_bottleneck = self._analyze_memory_bottleneck(analysis_results)
        if memory_bottleneck:
            bottlenecks.append(memory_bottleneck)
            confidence_factors.append(memory_bottleneck['confidence'])
        
        # Disk Analysis
        disk_bottleneck = self._analyze_disk_bottleneck(analysis_results)
        if disk_bottleneck:
            bottlenecks.append(disk_bottleneck)
            confidence_factors.append(disk_bottleneck['confidence'])
        
        # SQL Server specific bottlenecks
        sql_bottlenecks = self._analyze_sql_bottlenecks(analysis_results)
        bottlenecks.extend(sql_bottlenecks)
        confidence_factors.extend([b['confidence'] for b in sql_bottlenecks])
        
        # Calculate overall confidence
        overall_confidence = max(confidence_factors) if confidence_factors else 0.0
        
        # Sort by severity and confidence
        bottlenecks.sort(key=lambda x: (x['severity_score'], x['confidence']), reverse=True)
        
        return {
            'bottlenecks': bottlenecks[:5],  # Top 5 bottlenecks
            'confidence': overall_confidence,
            'method': 'local_analysis'
        }
    
    def _analyze_cpu_bottleneck(self, analysis_results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze CPU-related bottlenecks"""
        bottleneck = None
        
        # Check wait stats for CPU pressure
        wait_stats = analysis_results.get('wait_stats', {}).get('data', {})
        cpu_waits = wait_stats.get('cpu_related_waits', [])
        
        # Check PerfMon CPU data
        perfmon_data = analysis_results.get('perfmon_analysis', {})
        cpu_analysis = perfmon_data.get('cpu_analysis', {})
        cpu_metrics = cpu_analysis.get('metrics', {})
        
        cpu_usage = cpu_metrics.get('avg_processor_time', 0)
        cpu_queue = cpu_metrics.get('avg_processor_queue', 0)
        
        confidence = 0.0
        severity_score = 0
        
        if cpu_usage > 80:
            confidence += 0.4
            severity_score += 3
        if cpu_queue > 2:
            confidence += 0.3
            severity_score += 2
        if cpu_waits:
            confidence += 0.3
            severity_score += 2
        
        if confidence >= 0.5:
            bottleneck = {
                'component': 'CPU',
                'type': 'resource_contention',
                'description': f'High CPU usage ({cpu_usage:.1f}%) with queue length {cpu_queue:.1f}',
                'confidence': min(confidence, 1.0),
                'severity_score': severity_score,
                'evidence': {
                    'cpu_usage_pct': cpu_usage,
                    'cpu_queue_length': cpu_queue,
                    'cpu_waits': len(cpu_waits) if cpu_waits else 0
                },
                'questions_for_ai': [
                    "What specific queries or processes are consuming the most CPU?",
                    "Are there poorly optimized queries causing CPU spikes?",
                    "Is parallelism configured appropriately for this workload?"
                ]
            }
        
        return bottleneck
    
    def _analyze_memory_bottleneck(self, analysis_results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze memory-related bottlenecks"""
        bottleneck = None
        
        # Check PerfMon memory data
        perfmon_data = analysis_results.get('perfmon_analysis', {})
        memory_analysis = perfmon_data.get('memory_analysis', {})
        memory_metrics = memory_analysis.get('metrics', {})
        
        available_mb = memory_metrics.get('avg_available_mb', 1000)
        page_life_expectancy = memory_metrics.get('avg_page_life_expectancy', 300)
        
        confidence = 0.0
        severity_score = 0
        
        if available_mb < 200:
            confidence += 0.5
            severity_score += 4
        elif available_mb < 500:
            confidence += 0.3
            severity_score += 2
        
        if page_life_expectancy < 300:
            confidence += 0.4
            severity_score += 3
        elif page_life_expectancy < 600:
            confidence += 0.2
            severity_score += 1
        
        if confidence >= 0.5:
            bottleneck = {
                'component': 'Memory',
                'type': 'resource_shortage',
                'description': f'Low available memory ({available_mb:.0f} MB) and PLE ({page_life_expectancy:.0f}s)',
                'confidence': min(confidence, 1.0),
                'severity_score': severity_score,
                'evidence': {
                    'available_memory_mb': available_mb,
                    'page_life_expectancy_sec': page_life_expectancy
                },
                'questions_for_ai': [
                    "What is the optimal max memory setting for this server?",
                    "Are there memory-intensive queries causing pressure?",
                    "Should we investigate buffer pool utilization patterns?"
                ]
            }
        
        return bottleneck
    
    def _analyze_disk_bottleneck(self, analysis_results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Analyze disk I/O related bottlenecks"""
        bottleneck = None
        
        # Check disk performance from analysis
        disk_data = analysis_results.get('disk_performance', {}).get('data', {})
        io_stall_data = disk_data.get('io_stall_analysis', [])
        
        # Check PerfMon disk data
        perfmon_data = analysis_results.get('perfmon_analysis', {})
        disk_analysis = perfmon_data.get('disk_analysis', {})
        disk_metrics = disk_analysis.get('metrics', {})
        
        queue_length = disk_metrics.get('avg_disk_queue_length', 0)
        read_latency_ms = disk_metrics.get('avg_disk_read_ms', 0)
        
        confidence = 0.0
        severity_score = 0
        
        # Analyze disk queue length
        if queue_length > 10:
            confidence += 0.5
            severity_score += 4
        elif queue_length > 2:
            confidence += 0.3
            severity_score += 2
        
        # Analyze latency
        if read_latency_ms > 25:
            confidence += 0.4
            severity_score += 3
        elif read_latency_ms > 15:
            confidence += 0.2
            severity_score += 1
        
        # Check I/O stall from SQL analysis
        if io_stall_data:
            high_stall_files = [f for f in io_stall_data if f.get('avg_stall_ms', 0) > 20]
            if high_stall_files:
                confidence += 0.3
                severity_score += len(high_stall_files)
        
        if confidence >= 0.5:
            bottleneck = {
                'component': 'Disk I/O',
                'type': 'performance_degradation',
                'description': f'High disk latency ({read_latency_ms:.1f}ms) and queue length ({queue_length:.1f})',
                'confidence': min(confidence, 1.0),
                'severity_score': severity_score,
                'evidence': {
                    'disk_queue_length': queue_length,
                    'read_latency_ms': read_latency_ms,
                    'high_stall_files': len(high_stall_files) if 'high_stall_files' in locals() else 0
                },
                'questions_for_ai': [
                    "Which database files are experiencing the highest I/O stall?",
                    "Would separating data, log, and tempdb improve performance?",
                    "Are there opportunities for index optimization to reduce I/O?"
                ]
            }
        
        return bottleneck
    
    def _analyze_sql_bottlenecks(self, analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze SQL Server specific bottlenecks"""
        bottlenecks = []
        
        # Wait Statistics Analysis
        wait_stats = analysis_results.get('wait_stats', {}).get('data', {})
        top_waits = wait_stats.get('top_waits', [])
        
        for wait in top_waits[:3]:  # Top 3 waits
            wait_type = wait.get('wait_type', '')
            wait_time_s = wait.get('wait_time_s', 0)
            percentage = wait.get('percentage', 0)
            
            if percentage > 5:  # Significant wait
                confidence = min(percentage / 100 * 2, 1.0)  # Higher percentage = higher confidence
                
                bottleneck = {
                    'component': 'SQL Server',
                    'type': f'wait_type_{wait_type.lower()}',
                    'description': f'{wait_type} waits consuming {percentage:.1f}% of total wait time',
                    'confidence': confidence,
                    'severity_score': int(percentage / 10) + 1,
                    'evidence': {
                        'wait_type': wait_type,
                        'wait_time_seconds': wait_time_s,
                        'percentage_of_total': percentage
                    },
                    'questions_for_ai': [
                        f"What are the root causes of {wait_type} waits?",
                        f"How can we reduce {wait_type} wait times?",
                        "Are there query patterns contributing to these waits?"
                    ]
                }
                bottlenecks.append(bottleneck)
        
        return bottlenecks
    
    def _conduct_ai_dialog(self, context: DialogContext, local_results: Dict[str, Any]) -> DialogContext:
        """Conduct multi-turn AI dialog to clarify bottlenecks
        
        Args:
            context: Dialog context
            local_results: Results from local analysis
            
        Returns:
            Updated dialog context
        """
        self.logger.info("🔄 Starting AI dialog investigation...")
        
        # Start with local results
        context.identified_bottlenecks = local_results['bottlenecks']
        
        while (context.status == 'active' and 
               context.current_turn < context.max_turns and
               context.total_tokens_used < context.max_tokens_allowed):
            
            context.current_turn += 1
            self.logger.info(f"🤖 AI Dialog Turn {context.current_turn}/{context.max_turns}")
            
            # Generate questions for current bottlenecks
            questions = self._generate_clarifying_questions(context.identified_bottlenecks)
            
            if not questions:
                self.logger.info("✅ No more questions needed, analysis complete")
                context.status = 'completed'
                break
            
            # Ask AI to analyze and provide insights
            ai_response = self._ask_ai_for_insights(context, questions)
            
            if not ai_response:
                self.logger.warning("❌ AI failed to respond, ending dialog")
                context.status = 'aborted'
                break
            
            # Update token usage
            context.total_tokens_used += ai_response.get('tokens_used', 0)
            
            # Process AI response
            new_insights = self._process_ai_response(ai_response)
            
            # Update bottlenecks with new insights
            if new_insights:
                context.identified_bottlenecks = self._merge_insights(context.identified_bottlenecks, new_insights)
                context.confidence_score = self._calculate_overall_confidence(context.identified_bottlenecks)
                
                self.logger.info(f"🎯 Updated confidence: {context.confidence_score:.1%}")
                
                # Check if we have high confidence now
                if context.confidence_score >= self.confidence_threshold:
                    self.logger.info("✅ Achieved high confidence, ending dialog")
                    context.status = 'completed'
                    break
            
            # Add to conversation history
            context.conversation_history.append({
                'turn': context.current_turn,
                'questions': questions,
                'ai_response': ai_response,
                'timestamp': datetime.now().isoformat()
            })
            
            # Small delay between turns
            time.sleep(1)
        
        # Check why we exited the loop
        if context.total_tokens_used >= context.max_tokens_allowed:
            context.status = 'token_limit_reached'
            self.logger.warning(f"🚫 Token limit reached ({context.total_tokens_used}/{context.max_tokens_allowed})")
        elif context.current_turn >= context.max_turns:
            context.status = 'max_turns_reached'
            self.logger.warning(f"🚫 Maximum turns reached ({context.current_turn}/{context.max_turns})")
        
        self.logger.info(f"🏁 AI dialog completed with status: {context.status}")
        return context
    
    def _generate_clarifying_questions(self, bottlenecks: List[Dict[str, Any]]) -> List[str]:
        """Generate questions to clarify current bottlenecks"""
        questions = []
        
        for bottleneck in bottlenecks[:2]:  # Focus on top 2 bottlenecks
            if 'questions_for_ai' in bottleneck:
                questions.extend(bottleneck['questions_for_ai'][:2])  # Max 2 questions per bottleneck
        
        return questions[:self.max_questions_per_turn]
    
    def _ask_ai_for_insights(self, context: DialogContext, questions: List[str]) -> Optional[Dict[str, Any]]:
        """Ask AI for insights based on current questions"""
        try:
            # Create prompt with context and questions
            prompt = self._create_dialog_prompt(context, questions)
            
            # Call AI service
            ai_response = self.ai_service.client.chat.completions.create(
                model=self.ai_service.config.azure_openai_deployment,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert SQL Server performance analyst conducting a detailed investigation. Answer the specific questions with actionable insights and provide confidence scores for your recommendations."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                max_tokens=500,
                temperature=0.1
            )
            
            response_content = ai_response.choices[0].message.content.strip()
            tokens_used = ai_response.usage.total_tokens if hasattr(ai_response, 'usage') else 300
            
            return {
                'content': response_content,
                'tokens_used': tokens_used,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting AI insights: {e}")
            return None
    
    def _create_dialog_prompt(self, context: DialogContext, questions: List[str]) -> str:
        """Create dialog prompt with context and questions"""
        
        prompt_parts = [
            "SQL Server Performance Investigation - Dialog Turn {}".format(context.current_turn),
            "",
            "Current Identified Bottlenecks:"
        ]
        
        for i, bottleneck in enumerate(context.identified_bottlenecks[:3], 1):
            prompt_parts.append(f"{i}. {bottleneck['component']}: {bottleneck['description']} (Confidence: {bottleneck['confidence']:.1%})")
        
        prompt_parts.extend([
            "",
            "Questions for Investigation:",
        ])
        
        for i, question in enumerate(questions, 1):
            prompt_parts.append(f"{i}. {question}")
        
        prompt_parts.extend([
            "",
            "Please provide:",
            "1. Specific answers to each question",
            "2. Root cause analysis",
            "3. Actionable recommendations", 
            "4. Confidence score (0-1) for each recommendation",
            "",
            "Format as JSON: {'answers': [{'question': 'Q', 'answer': 'A', 'confidence': 0.8}], 'recommendations': [{'action': 'X', 'confidence': 0.9}]}"
        ])
        
        return "\n".join(prompt_parts)
    
    def _process_ai_response(self, ai_response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process AI response and extract insights"""
        try:
            content = ai_response['content']
            
            # Try to parse as JSON
            try:
                insights = json.loads(content)
                return insights
            except json.JSONDecodeError:
                # Fallback: extract insights from text
                return {
                    'raw_response': content,
                    'confidence': 0.6  # Medium confidence for text responses
                }
                
        except Exception as e:
            self.logger.error(f"Error processing AI response: {e}")
            return None
    
    def _merge_insights(self, current_bottlenecks: List[Dict[str, Any]], new_insights: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Merge new AI insights with current bottlenecks"""
        # This is a simplified merge - in production, you'd want more sophisticated logic
        
        if 'recommendations' in new_insights:
            for rec in new_insights['recommendations']:
                # Add as new bottleneck or update existing
                bottleneck = {
                    'component': 'AI Analysis',
                    'type': 'ai_recommendation',
                    'description': rec.get('action', 'AI recommendation'),
                    'confidence': rec.get('confidence', 0.7),
                    'severity_score': int(rec.get('confidence', 0.7) * 5),
                    'source': 'ai_dialog'
                }
                current_bottlenecks.append(bottleneck)
        
        # Sort by confidence and severity
        current_bottlenecks.sort(key=lambda x: (x['confidence'], x['severity_score']), reverse=True)
        return current_bottlenecks[:5]  # Keep top 5
    
    def _calculate_overall_confidence(self, bottlenecks: List[Dict[str, Any]]) -> float:
        """Calculate overall confidence score"""
        if not bottlenecks:
            return 0.0
        
        # Weighted average with higher weight for top bottlenecks
        weights = [1.0, 0.8, 0.6, 0.4, 0.2]
        
        total_weighted_confidence = 0.0
        total_weight = 0.0
        
        for i, bottleneck in enumerate(bottlenecks[:5]):
            weight = weights[i] if i < len(weights) else 0.1
            confidence = bottleneck.get('confidence', 0.0)
            
            total_weighted_confidence += confidence * weight
            total_weight += weight
        
        return total_weighted_confidence / total_weight if total_weight > 0 else 0.0