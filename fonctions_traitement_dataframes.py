import pandas as pd
import pdfplumber
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

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

class PDFPlumberExtractor:
    def extract_ranges(self, pdf_path: str, page_ranges: List[str]) -> List[pd.DataFrame]:
        try:
            print(f"    📄 PDFPlumber: extraction plages {page_ranges}")
            
            all_pages = PageRangeParser.parse_multiple_ranges(page_ranges)
            tables = []
            
            with pdfplumber.open(pdf_path) as pdf:
                for page_num in all_pages:
                    if page_num <= len(pdf.pages):
                        page = pdf.pages[page_num - 1]
                        page_tables = page.extract_tables()
                        
                        for table in page_tables:
                            if table and len(table) > 1:
                                cleaned_table = []
                                for row in table:
                                    cleaned_row = [cell if cell is not None else "" for cell in row]
                                    cleaned_table.append(cleaned_row)
                                
                                if cleaned_table:
                                    df = pd.DataFrame(cleaned_table[1:], columns=cleaned_table[0])
                                    tables.append(df)
            
            print(f"      ✅ {len(tables)} tableaux extraits avec PDFPlumber")
            return tables
            
        except Exception as e:
            print(f"      ❌ Erreur PDFPlumber: {e}")
            return []

class DataCleaner:
    def __init__(self, cleaning_rules: Dict[str, Any]):
        self.rules = cleaning_rules
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
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
            tables = self.pdfplumber_extractor.extract_ranges(self.config.pdf_path, page_ranges)
            all_tables.extend(tables)
        
        if not all_tables:
            return None
        
        cleaned_tables = [self.cleaner.clean_dataframe(table) for table in all_tables]
        cleaned_tables = [table for table in cleaned_tables if not table.empty]
        
        if not cleaned_tables:
            return None
        
        final_df = self._combine_tables(cleaned_tables)
        final_df = self._apply_transformations(final_df)
        
        return final_df
    
    def _combine_tables(self, tables: List[pd.DataFrame]) -> pd.DataFrame:
        if len(tables) == 1:
            return tables[0]
        
        try:
            return pd.concat(tables, ignore_index=True, sort=False)
        except:
            return max(tables, key=len)
    
    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
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
