"""
Classes de traitement et nettoyage des données
"""

import pandas as pd
import re
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from extractors import PDFPlumberExtractor
from config import DICO_BORDEREAU


@dataclass
class DictionaryExtractionConfig:
    pdf_path: str
    page_ranges_dict: Dict[str, List[str]]
    output_directory: str = "extracted_categories"
    extraction_methods: List[str] = None
    cleaning_rules: Dict[str, Any] = None
    column_mapping: Dict[str, str] = None
    filters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.extraction_methods is None:
            self.extraction_methods = ["pdfplumber"]
        if self.cleaning_rules is None:
            self.cleaning_rules = {}
        if self.column_mapping is None:
            self.column_mapping = {}
        if self.filters is None:
            self.filters = {}


class DataCleaner:
    def __init__(self, cleaning_rules: Dict[str, Any]):
        self.rules = cleaning_rules
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
            
        df_clean = df.copy()
        
        if self.rules.get('remove_empty_rows', True):
            df_clean = df_clean.dropna(how='all')
        
        if self.rules.get('remove_empty_columns', True):
            df_clean = df_clean.dropna(axis=1, how='all')
        
        if self.rules.get('strip_whitespace', True):
            df_clean = df_clean.map(
                lambda x: x.strip() if isinstance(x, str) else x
            )
        
        regex_rules = self.rules.get('regex_patterns', {})
        for column, patterns in regex_rules.items():
            if column in df_clean.columns:
                for pattern, replacement in patterns.items():
                    df_clean[column] = df_clean[column].astype(str).str.replace(
                        pattern, replacement, regex=True
                    )
        
        return df_clean


class CategoryProcessor:
    def __init__(self, config: DictionaryExtractionConfig):
        self.config = config
        self.pdfplumber_extractor = PDFPlumberExtractor()
        self.cleaner = DataCleaner(config.cleaning_rules)
    
    def process_category(self, category_name: str, page_ranges: List[str]) -> Optional[pd.DataFrame]:
        all_tables = []
        
        for method in self.config.extraction_methods:
            tables = self.pdfplumber_extractor.extract_ranges(self.config.pdf_path, page_ranges, category_name)
            all_tables.extend(tables)
        
        if not all_tables:
            return None
        
        cleaned_tables = [self.cleaner.clean_dataframe(table) for table in all_tables]
        cleaned_tables = [table for table in cleaned_tables if table is not None and not table.empty]
        
        if not cleaned_tables:
            return None
        
        final_df = self._combine_tables(cleaned_tables)
        final_df = self._apply_transformations(final_df)
        
        return final_df
    
    def _combine_tables(self, tables: List[pd.DataFrame]) -> pd.DataFrame:
        if len(tables) == 1:
            return tables[0].reset_index(drop=True)
        
        try:
            clean_tables = []
            for table in tables:
                if table is not None and not table.empty:
                    clean_tables.append(table.reset_index(drop=True))
            
            if clean_tables:
                return pd.concat(clean_tables, ignore_index=True, sort=False)
            else:
                return pd.DataFrame()
        except Exception as e:
            print(f"Erreur combinaison tables: {e}")
            return max(tables, key=len).reset_index(drop=True) if tables else pd.DataFrame()
    
    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
            
        if self.config.column_mapping:
            df = df.rename(columns=self.config.column_mapping)
        
        for column, filter_config in self.config.filters.items():
            if column not in df.columns:
                continue
                
            filter_type = filter_config.get('type', 'contains')
            filter_value = filter_config.get('value', '')
            
            if filter_type == 'contains':
                df = df[df[column].astype(str).str.contains(filter_value, na=False)]
            elif filter_type == 'equals':
                df = df[df[column] == filter_value]
            elif filter_type == 'not_empty':
                df = df[df[column].notna()]
        
        return df
