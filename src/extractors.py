"""
Extracteurs de données depuis les PDF
"""

import pdfplumber
import pandas as pd
import PyPDF2
from typing import List, Optional

from src.processors import DataFrameCombiner
from .utils import PageRangeParser
from config import BORDEREAU_A5_COLUMNS
import re


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
    
    def _extract_tables_from_page(self, pdf, page_num: int, pdf_path: str, category_name: str) -> List[pd.DataFrame]:
        """
        Extrait les tableaux d'une page spécifique
        """
        page = pdf.pages[page_num - 1]
        page_tables = page.extract_tables()
        tables = []
        
        for i, table in enumerate(page_tables):
            if table and len(table) > 1:
                # Nettoyer les données du tableau
                cleaned_table = self._clean_table_data(table)
                
                if cleaned_table:
                    df = pd.DataFrame(cleaned_table[1:], columns=cleaned_table[0])
                    
                    # VALIDATION AJOUTÉE
                    df = self._validate_dataframe(df, f"page_{page_num}_table_{i}")
                    
                    # Traitement spécial pour Bordereau A5
                    if category_name == "Bordereau A5 n":
                        df = self._enhance_bordereau_a5(df, pdf_path, page_num)
                        # Re-valider après enhancement
                        df = self._validate_dataframe(df, f"page_{page_num}_A5_enhanced")
                    
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

        pattern_line1 = r"Emploi : (.*?) Lieu de travail (.*?) Publié sous le n° (.+)"
        pattern_line3 = r"Motif (.*?) Position (.*?) GF de publication (.+)"
        pattern_line4 = r"CERNE\s*:\s*(.*?)\s+Référence MyHR\s+(.+)"
        
        for line in lines:
            line = line.strip()
            
            if not line:
                continue

            if line.startswith('UM :'):
                parts = line.split(' ')
                if len(parts) >= 3:
                    metadata["UM_code"] = parts[2].strip()
                    metadata["UM_char"] = ' '.join(parts[3:]).strip() if len(parts) > 3 else None

            elif line.startswith('DUM :'):
                parts = line.split(' ')
                if len(parts) >= 3:
                    metadata["DUM_code"] = parts[2].strip()
                    metadata["DUM_char"] = ' '.join(parts[3:]).strip() if len(parts) > 3 else None

            elif line.startswith('SDUM :'):
                parts = line.split(' ')
                if len(parts) >= 3:
                    metadata["SDUM_code"] = parts[2].strip()
                    metadata["SDUM_char"] = ' '.join(parts[3:]).strip() if len(parts) > 3 else None

            elif line.startswith('FSDUM :'):
                parts = line.split(' ')
                if len(parts) >= 3:
                    metadata["FSDUM_code"] = parts[2].strip()
                    metadata["FSDUM_char"] = ' '.join(parts[3:]).strip() if len(parts) > 3 else None

            elif line.startswith('Emploi :'):
                match1 = re.search(pattern_line1, line)
                if match1:
                    metadata["Emploi"] = match1.group(1).strip()
                    metadata["Lieu_de_travail"] = match1.group(2).strip()
                    metadata["Publie_sous_le"] = match1.group(3).strip()

            elif line.startswith("Nombre d'emploi(s) "):
                parts = line.split(' ')
                if len(parts) >= 3:
                    Nombre_demploi = parts[2].strip()
                    if len(parts) > 3:
                        remaining = ' '.join(parts[3:])
                        if "Date de forclusion" in remaining:
                            location_part = remaining.split("Date de forclusion")[0].strip()
                            date_part = remaining.split("Date de forclusion")[1].strip()
                            if metadata["Lieu_de_travail"] and location_part:
                                metadata["Lieu_de_travail"] = metadata["Lieu_de_travail"] + ' ' + location_part
                            elif location_part:
                                metadata["Lieu_de_travail"] = location_part
                            metadata["Date_de_forclusion"] = date_part

            elif line.startswith('Motif '):
                match3 = re.search(pattern_line3, line)
                if match3:
                    metadata["Motif"] = match3.group(1).strip()
                    metadata["Position"] = match3.group(2).strip()
                    metadata["GF_de_publication"] = match3.group(3).strip()

            elif line.startswith('CERNE :'):
                # Nettoyage des espaces insécables et normalisation des espaces
                line_clean = line.replace('\xa0', ' ')
                line_clean = re.sub(r'\s+', ' ', line_clean)
                match4 = re.search(pattern_line4, line_clean)
                if match4:
                    metadata["CERNE"] = match4.group(1).strip()
                    metadata["Reference_My_HR"] = match4.group(2).strip()
        
        return metadata
    
    def _validate_dataframe(self, df: pd.DataFrame, source: str = "unknown") -> pd.DataFrame:
        """
        Valide et nettoie un DataFrame pour éviter les problèmes d'index
        
        Args:
            df: DataFrame à valider
            source: Source du DataFrame pour le debug
            
        Returns:
            DataFrame validé
        """
        if df is None or df.empty:
            return df
        
        print(f"    🔍 Validation DataFrame de {source}:")
        print(f"      Shape: {df.shape}")
        print(f"      Index unique: {df.index.is_unique}")
        print(f"      Colonnes dupliquées: {df.columns.duplicated().any()}")
        
        # Forcer un index propre
        df_clean = df.reset_index(drop=True)
        
        # Gérer les colonnes dupliquées
        if df_clean.columns.duplicated().any():
            print(f"      ⚠️ Renommage des colonnes dupliquées")
            df_clean.columns = DataFrameCombiner._make_unique_columns(df_clean.columns)
        
        return df_clean



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
