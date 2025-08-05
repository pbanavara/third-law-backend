from abc import ABC, abstractmethod
from typing import Dict, Any, List
import re
import time
import logging

logger = logging.getLogger(__name__)

class TextHandler(ABC):
    @abstractmethod
    def process(self, text: str) -> List[Dict[str, Any]]:
        pass

class RegexHandler(TextHandler):
    def __init__(self):
        # Compile regex patterns once at initialization
        self.patterns = {
            'email': re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'),
            'ssn': re.compile(r'\b\d{3}-?\d{2}-?\d{4}\b')
        }

    def process(self, text: str) -> List[Dict[str, Any]]:
        findings = []
        for pattern_type, pattern in self.patterns.items():
            findings.extend(self._find_matches(text, pattern, pattern_type))
        return findings

    def _find_matches(self, text: str, pattern: re.Pattern, pattern_type: str) -> List[Dict[str, Any]]:
        matches = pattern.finditer(text)
        findings = []
        
        for match in matches:
            findings.append({
                'type': pattern_type,
                'value': match.group(),
                'start': match.start(),
                'end': match.end()
            })
        
        return findings

class PDFTextProcessor:
    def __init__(self):
        self.handlers = [RegexHandler()]

    def process_text(self, text: str) -> Dict[str, Any]:
        start_time = time.time()
        
        all_findings = []
        findings_by_type = {}
        
        for handler in self.handlers:
            findings = handler.process(text)
            all_findings.extend(findings)
            
            # Group findings by type
            for finding in findings:
                finding_type = finding['type']
                if finding_type not in findings_by_type:
                    findings_by_type[finding_type] = 0
                findings_by_type[finding_type] += 1
        
        process_time = time.time() - start_time
        logger.info(f"Total text processing took {process_time:.3f}s")
        
        return {
            'success': True,
            'findings': all_findings,
            'statistics': {
                'total_chars_processed': len(text),
                'handlers_used': len(self.handlers),
                'total_findings': len(all_findings),
                'findings_by_type': findings_by_type,
                'processing_time': process_time
            }
        } 