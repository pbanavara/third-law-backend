from abc import ABC, abstractmethod
import re
from typing import List, Dict, Any
from dataclasses import dataclass

@dataclass
class SensitiveInfo:
    """Data class to store sensitive information findings"""
    info_type: str
    value: str
    location: int  # character position in text

class TextHandler(ABC):
    """Abstract base class for text handlers"""
    @abstractmethod
    def process(self, text: str) -> List[SensitiveInfo]:
        """Process text and return list of sensitive information found"""
        pass

class RegexHandler(TextHandler):
    """Handler that uses regex patterns to identify sensitive information"""
    
    # Regex patterns for different types of sensitive information
    PATTERNS = {
        'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
        'ssn': r'\b\d{3}-?\d{2}-?\d{4}\b'
    }
    
    def process(self, text: str) -> List[SensitiveInfo]:
        findings = []
        
        # Process each pattern
        for info_type, pattern in self.PATTERNS.items():
            # Find all matches with their positions
            for match in re.finditer(pattern, text):
                findings.append(
                    SensitiveInfo(
                        info_type=info_type,
                        value=match.group(),
                        location=match.start()
                    )
                )
        
        return findings

class PDFTextProcessor:
    """Main class for processing PDF text"""
    
    def __init__(self):
        # Initialize handlers
        self.handlers = [
            RegexHandler(),
            # Future: Add HeuristicsHandler and LLMHandler
        ]
    
    def process_text(self, text: str) -> Dict[str, Any]:
        """
        Process text through all handlers and return findings
        
        Returns:
            Dict containing:
            - success: bool
            - findings: List of sensitive information found
            - statistics: Dict with processing statistics
        """
        all_findings = []
        stats = {
            'total_chars_processed': len(text),
            'handlers_used': len(self.handlers)
        }
        
        try:
            # Process text through each handler
            for handler in self.handlers:
                findings = handler.process(text)
                all_findings.extend(findings)
            
            # Add statistics about findings
            stats['total_findings'] = len(all_findings)
            stats['findings_by_type'] = {}
            for finding in all_findings:
                stats['findings_by_type'][finding.info_type] = \
                    stats['findings_by_type'].get(finding.info_type, 0) + 1
            
            return {
                'success': True,
                'findings': [
                    {
                        'type': f.info_type,
                        'value': f.value,
                        'location': f.location
                    } for f in all_findings
                ],
                'statistics': stats
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'statistics': stats
            } 