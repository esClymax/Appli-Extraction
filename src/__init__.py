"""
Extracteur Multi-PDF vers CSV
"""

__version__ = "1.0.0"
__author__ = "Maxime Faure"
__description__ = "Extracteur de tableaux PDF vers CSV avec consolidation globale"

# Imports pour faciliter l'utilisation
from .pdf_analyzer import PDFAnalyzer, CoverageAnalyzer
from .extractors import PDFTableExtractor, MultiMethodExtractor
from .processors import DataFrameProcessor, DataFrameCombiner
from .csv_generator import CSVGenerator, GlobalCSVGenerator, ExtractionConfig
from .streamlit_ui import StreamlitUI
from .utils import FileNameSanitizer, PageRangeParser, OutputCapture
