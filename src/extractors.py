"""
Extracteurs de données depuis les PDF
"""

import pdfplumber
import pandas as pd
import PyPDF2
from typing import List, Optional
from .utils import PageRangeParser
from config import BORDEREAU_A5_COLUMNS


class PDFTableExtractor:
    """Extracteur de tableaux depuis PDF avec PDFPlumber"""
    
    def extract_tables_from_ranges(self, pdf_path: str, page_ranges: List[str], 
                                   category_name: str) -> List[pd.DataFrame]:
        """
        Extrait les tableaux des plages de pages spécifiées
        
        Args:
            pdf_path: Chemin vers le PDF
            page_ranges: Liste des plages de pages
            category_name: Nom de la catégorie
            
        Returns:
            Liste des DataFrames extraits
        """
        try:
            print(f"    📄 PDFPlumber: extraction plages {page_ranges}")
            
            all_pages = PageRangeParser.parse_multiple_ranges(page_ranges)
            tables = []
            
            with pdfplumber.open(pdf_path) as pdf:
                for page_num in all_pages:
                    if page_num <= len(pdf.pages):
                        page_tables = self._extract_tables_from_page(
                            pdf, page_num, pdf_path, category_name
                        )
                        tables.extend(page_tables)
            
            print(f"      ✅ {len(tables)} tableaux extraits avec PDFPlumber")
            return tables
            
        except Exception as e:
            print(f"      ❌ Erreur PDFPlumber: {e}")
            return []
    
    def _extract_tables_from_page(self, pdf, page_num: int, pdf_path: str, 
                                  category_name: str) -> List[pd.DataFrame]:
        """
        Extrait les tableaux d'une page spécifique
        
        Args:
            pdf: Objet PDF pdfplumber
            page_num: Numéro de page
            pdf_path: Chemin du PDF
            category_name: Nom de la catégorie
            
        Returns:
            Liste des DataFrames de la page
        """
        page = pdf.pages[page_num - 1]
        page_tables = page.extract_tables()
        tables = []
        
        for table in page_tables:
            if table and len(table) > 1:
                # Nettoyer les données du tableau
                cleaned_table = self._clean_table_data(table)
                
                if cleaned_table:
                    df = pd.DataFrame(cleaned_table[1:], columns=cleaned_table[0])
                    
                    # Traitement spécial pour Bordereau A5
                    if category_name == "Bordereau A5 n":
                        df = self._enhance_bordereau_a5(df, pdf_path, page_num)
                    
                    tables.append(df)
        
        return tables
    
    def _clean_table_data(self, table: List[List]) -> List[List]:
        """
        Nettoie les données brutes du tableau
        
        Args:
            table: Données brutes du tableau
            
        Returns:
            Données nettoyées
        """
        cleaned_table = []
        for row in table:
            cleaned_row = [cell if cell is not None else "" for cell in row]
            cleaned_table.append(cleaned_row)
        return cleaned_table
    
    def _enhance_bordereau_a5(self, df: pd.DataFrame, pdf_path: str, 
                              page_num: int) -> pd.DataFrame:
        """
        Enrichit les données du Bordereau A5 avec des informations contextuelles
        
        Args:
            df: DataFrame de base
            pdf_path: Chemin du PDF
            page_num: Numéro de page
            
        Returns:
            DataFrame enrichi
        """
        try:
            # Extraire le texte de la page pour les métadonnées
            with open(pdf_path, 'rb') as fichier:
                lecteur = PyPDF2.PdfReader(fichier)
                page = lecteur.pages[page_num - 1]
                page_text = page.extract_text()
            
            # Parser les métadonnées
            metadata = self._parse_bordereau_a5_metadata(page_text)
            
            # Créer le DataFrame des métadonnées
            metadata_values = [[metadata[col] for col in BORDEREAU_A5_COLUMNS]] * len(df)
            metadata_df = pd.DataFrame(data=metadata_values, columns=BORDEREAU_A5_COLUMNS)
            
            # Combiner avec les données originales
            enhanced_df = pd.concat([df, metadata_df], axis=1)
            return enhanced_df
            
        except Exception as e:
            print(f"      ⚠️ Erreur enrichissement Bordereau A5: {e}")
            return df
    
    def _parse_bordereau_a5_metadata(self, page_text: str) -> dict:
        """
        Parse les métadonnées du Bordereau A5
        
        Args:
            page_text: Texte de la page
            
        Returns:
            Dictionnaire des métadonnées
        """
        lines = page_text.split('\n')
        metadata = {col: None for col in BORDEREAU_A5_COLUMNS}
        
        for line in lines:
            line = line.strip()
            
            if line.startswith('UM :'):
                metadata['UM'] = line.split('UM :', 1)[1].strip()
            elif line.startswith('DUM :'):
                metadata['DUM'] = line.split('DUM :', 1)[1].strip()
            elif line.startswith('SDUM :'):
                metadata['SDUM'] = line.split('SDUM :', 1)[1].strip()
            elif line.startswith('Emploi :'):
                metadata['Emploi_Lieu_de_travail_Publié_sous_le'] = line
            elif line.startswith("Nombre d'emploi(s) "):
                metadata['Nombre_demploi_Lieu_de_travail_Date_de_forclusion'] = line
            elif line.startswith('Motif '):
                metadata['Motif_Position_GF_de_publication'] = line
            elif line.startswith('CERNE :'):
                metadata['CERNE_Reference_My_HR'] = line.replace('\xa0', '')
        
        return metadata


class MultiMethodExtractor:
    """Extracteur utilisant plusieurs méthodes d'extraction"""
    
    def __init__(self):
        self.pdf_extractor = PDFTableExtractor()
        # Ici on pourrait ajouter d'autres extracteurs (Camelot, Tabula, etc.)
    
    def extract_with_all_methods(self, pdf_path: str, page_ranges: List[str],
                                 category_name: str, methods: List[str] = None) -> List[pd.DataFrame]:
        """
        Extrait avec toutes les méthodes disponibles
        
        Args:
            pdf_path: Chemin du PDF
            page_ranges: Plages de pages
            category_name: Nom de la catégorie
            methods: Méthodes à utiliser (par défaut: ['pdfplumber'])
            
        Returns:
            Liste consolidée des DataFrames
        """
        if methods is None:
            methods = ['pdfplumber']
        
        all_tables = []
        
        for method in methods:
            if method == 'pdfplumber':
                tables = self.pdf_extractor.extract_tables_from_ranges(
                    pdf_path, page_ranges, category_name
                )
                all_tables.extend(tables)
        
        return all_tables
