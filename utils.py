"""
Fonctions utilitaires
"""

import re
import io
import sys
import PyPDF2
from typing import List


class FileNameSanitizer:
    @staticmethod
    def sanitize_filename(name: str) -> str:
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
        sanitized = re.sub(r'\s+', '_', sanitized)
        sanitized = sanitized.strip('._-')
        sanitized = sanitized[:50] if len(sanitized) > 50 else sanitized
        return sanitized


class PageRangeParser:
    @staticmethod
    def parse_range(page_range: str) -> List[int]:
        if '-' in page_range:
            start, end = page_range.split('-')
            return list(range(int(start), int(end) + 1))
        else:
            return [int(page_range)]
    
    @staticmethod
    def parse_multiple_ranges(page_ranges: List[str]) -> List[int]:
        all_pages = []
        for range_str in page_ranges:
            all_pages.extend(PageRangeParser.parse_range(range_str))
        return sorted(list(set(all_pages)))


def capture_prints(func, *args, **kwargs):
    """Capture les prints d'une fonction"""
    old_stdout = sys.stdout
    captured_output = io.StringIO()
    
    try:
        sys.stdout = captured_output
        result = func(*args, **kwargs)
        output = captured_output.getvalue()
        return result, output
    finally:
        sys.stdout = old_stdout


def calculate_coverage_info(pdf_path, dictionnaire_plages):
    """Calculer les informations de recouvrement du document"""
    try:
        with open(pdf_path, 'rb') as fichier:
            lecteur_pdf = PyPDF2.PdfReader(fichier)
            total_pages = len(lecteur_pdf.pages)
        
        pages_traitees = set()
        for category, page_ranges in dictionnaire_plages.items():
            if page_ranges:
                for range_str in page_ranges:
                    pages_range = PageRangeParser.parse_range(range_str)
                    pages_traitees.update(pages_range)
        
        toutes_pages = set(range(1, total_pages + 1))
        pages_non_traitees = toutes_pages - pages_traitees
        
        pourcentage_couverture = (len(pages_traitees) / total_pages) * 100 if total_pages > 0 else 0
        
        coverage_info = {
            'total_pages': total_pages,
            'pages_traitees': sorted(list(pages_traitees)),
            'pages_non_traitees': sorted(list(pages_non_traitees)),
            'nb_pages_traitees': len(pages_traitees),
            'nb_pages_non_traitees': len(pages_non_traitees),
            'pourcentage_couverture': round(pourcentage_couverture, 1)
        }
        
        return coverage_info
        
    except Exception as e:
        print(f"❌ Erreur lors du calcul de couverture : {e}")
        return {
            'total_pages': 0,
            'pages_traitees': [],
            'pages_non_traitees': [],
            'nb_pages_traitees': 0,
            'nb_pages_non_traitees': 0,
            'pourcentage_couverture': 0
        }
