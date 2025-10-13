"""
Générateur de fichiers CSV
"""

import pandas as pd
import io
import os
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from .processors import DataFrameProcessor, DataFrameCombiner
from .extractors import MultiMethodExtractor
from .utils import FileNameSanitizer
from config import DEFAULT_CLEANING_RULES, DICO_BORDEREAU


class CSVGenerator:
    """Générateur de fichiers CSV individuels et consolidés"""
    
    def __init__(self, extraction_config):
        self.config = extraction_config
        self.extractor = MultiMethodExtractor()
        self.processor = DataFrameProcessor(extraction_config.cleaning_rules)
        os.makedirs(extraction_config.output_directory, exist_ok=True)
    
    def generate_individual_csv(self, pdf_filename: str) -> Tuple[Optional[str], Dict, int, Optional[bytes], Optional[pd.DataFrame]]:
        """
        Génère un fichier CSV pour un PDF individuel
        
        Args:
            pdf_filename: Nom du fichier PDF
            
        Returns:
            Tuple (chemin_csv, résultats_traitement, succès_count, données_csv, dataframe_fusionné)
        """
        print(f"🟢 Début du traitement de {len(self.config.page_ranges_dict)} catégories")
        print(f"📁 Répertoire de sortie: {self.config.output_directory}")
        
        # Préparer le nom du fichier CSV
        base_name = os.path.splitext(pdf_filename)[0]
        safe_base_name = FileNameSanitizer.sanitize_filename(base_name)
        csv_filename = f"{safe_base_name}.csv"
        csv_filepath = os.path.join(self.config.output_directory, csv_filename)
        
        # Traiter chaque catégorie
        all_dataframes = []
        processing_results = {}
        success_count = 0
        
        for category_name, page_ranges in self.config.page_ranges_dict.items():
            result = self._process_category(category_name, page_ranges, pdf_filename)
            
            if result['success']:
                all_dataframes.append(result['dataframe'])
                success_count += 1
            
            processing_results[category_name] = result
        
        # Générer le CSV final
        if all_dataframes:
            return self._create_csv_file(all_dataframes, csv_filepath, csv_filename, 
                                       success_count, processing_results)
        else:
            print("❌ Aucune donnée à écrire dans le fichier CSV")
            return None, processing_results, 0, None, None
    
def _process_category(self, category_name: str, page_ranges: List[str], 
                     pdf_filename: str) -> Dict[str, Any]:
    """
    Traite une catégorie spécifique - VERSION SÉCURISÉE
    """
    print(f"\n🔍 Traitement de la catégorie: '{category_name}'")
    
    try:
        # Extraire les tableaux
        tables = self.extractor.extract_with_all_methods(
            self.config.pdf_path, page_ranges, category_name, 
            self.config.extraction_methods
        )
        
        if not tables:
            print(f"    ❌ Aucun tableau extrait pour {category_name}")
            # Créer un DataFrame vide mais valide
            empty_df = pd.DataFrame({
                'temp_col': [f'Aucun tableau trouvé dans {category_name}']
            })
            processed_df = self.processor.process_dataframe(empty_df, category_name, pdf_filename)
        else:
            # Combiner les tableaux
            combined_df = DataFrameCombiner.combine_tables(tables)
            
            # Traiter le DataFrame
            processed_df = self.processor.process_dataframe(
                combined_df, category_name, pdf_filename
            )
        
        # VÉRIFICATION : processed_df ne doit JAMAIS être None
        if processed_df is None:
            raise ValueError("Le processeur a retourné None")
        
        if processed_df.empty:
            print(f"    ⚠️ DataFrame vide après traitement pour {category_name}")
        else:
            print(f"    ✅ Préparé: {category_name} ({processed_df.shape[0]} lignes, {processed_df.shape[1]} colonnes)")
        
        return {
            'success': True,
            'dataframe': processed_df,
            'category_label': category_name,
            'rows': len(processed_df),
            'cols': len(processed_df.columns)
        }
        
    except Exception as e:
        print(f"    ❌ Erreur catégorie '{category_name}': {e}")
        # Créer un DataFrame d'erreur au lieu de None
        error_df = pd.DataFrame({
            'Document': [pdf_filename.replace('.pdf', '')],
            'Catégorie': [DICO_BORDEREAU.get(category_name, category_name)],
            'Nom & Prénom': [f'Erreur extraction: {str(e)}']
        })
        
        return {
            'success': True,  # Considéré comme succès car on a un DataFrame
            'dataframe': error_df,
            'category_label': category_name,
            'rows': len(error_df),
            'cols': len(error_df.columns),
            'error': str(e)
        }
    
    def _create_csv_file(self, dataframes: List[pd.DataFrame], csv_filepath: str,
                    csv_filename: str, success_count: int, 
                    processing_results: Dict) -> Tuple[str, Dict, int, bytes, pd.DataFrame]:
        """
        Crée le fichier CSV final
        """
        try:
            print(f"🔗 Création du CSV avec {len(dataframes)} DataFrames...")
            
            # Vérifier qu'on a des DataFrames valides
            valid_dataframes = []
            for i, df in enumerate(dataframes):
                if df is not None and not df.empty:
                    print(f"   DataFrame {i+1}: {len(df)} lignes, {len(df.columns)} colonnes")
                    valid_dataframes.append(df)
                else:
                    print(f"   DataFrame {i+1}: VIDE ou None - ignoré")
            
            if not valid_dataframes:
                print("❌ Aucun DataFrame valide à traiter")
                # Créer un DataFrame vide avec les colonnes de base
                empty_df = pd.DataFrame(columns=['Document', 'Catégorie', 'Nom & Prénom'])
                return self._write_csv_file(empty_df, csv_filepath, csv_filename, success_count, processing_results)
            
            # Fusionner tous les DataFrames
            merged_df = DataFrameCombiner.concatenate_all_dataframes(valid_dataframes)
            
            # VÉRIFICATION CRITIQUE : s'assurer que merged_df n'est pas None
            if merged_df is None:
                print("⚠️ concatenate_all_dataframes a retourné None - création d'un DataFrame vide")
                merged_df = pd.DataFrame(columns=['Document', 'Catégorie', 'Nom & Prénom'])
            elif merged_df.empty:
                print("⚠️ DataFrame fusionné est vide")
                # Garder le DataFrame vide mais avec les bonnes colonnes
                if 'Document' not in merged_df.columns:
                    merged_df = pd.DataFrame(columns=['Document', 'Catégorie', 'Nom & Prénom'])
            
            return self._write_csv_file(merged_df, csv_filepath, csv_filename, success_count, processing_results)
            
        except Exception as e:
            print(f"❌ Erreur lors de la création du fichier CSV: {e}")
            print(f"Type d'erreur: {type(e).__name__}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return None, processing_results, 0, None, None

    def _write_csv_file(self, merged_df: pd.DataFrame, csv_filepath: str, 
                    csv_filename: str, success_count: int, 
                    processing_results: Dict) -> Tuple[str, Dict, int, bytes, pd.DataFrame]:
        """
        Écrit le DataFrame dans un fichier CSV
        """
        try:
            # Vérification finale avant écriture
            if merged_df is None:
                raise ValueError("DataFrame est None - impossible d'écrire le CSV")
            
            print(f"📝 Écriture CSV: {len(merged_df)} lignes, {len(merged_df.columns)} colonnes")
            
            # Créer le contenu CSV
            csv_buffer = io.StringIO()
            merged_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_data = csv_buffer.getvalue().encode('utf-8-sig')
            
            # Sauvegarder le fichier
            with open(csv_filepath, 'w', encoding='utf-8-sig', newline='') as f:
                f.write(csv_buffer.getvalue())
            
            print(f"\n✅ Fichier CSV créé avec succès: {csv_filename}")
            print(f"📊 {len(merged_df)} lignes totales, {len(merged_df.columns)} colonnes")
            print(f"\n📊 RÉSUMÉ:")
            print(f"   ✅ {success_count}/{len(self.config.page_ranges_dict)} catégories traitées avec succès")
            print(f"   📁 Fichier CSV: {csv_filepath}")
            
            return csv_filepath, processing_results, success_count, csv_data, merged_df
            
        except Exception as e:
            print(f"❌ Erreur lors de l'écriture du CSV: {e}")
            return None, processing_results, 0, None, None


class GlobalCSVGenerator:
    """Générateur de CSV global consolidé"""
    
    @staticmethod
    def create_global_csv(all_results: Dict[str, Dict]) -> Optional[bytes]:
        """
        Crée un CSV global consolidant toutes les données de tous les PDF
        
        Args:
            all_results: Dictionnaire des résultats de tous les PDF
            
        Returns:
            Données du CSV global ou None
        """
        print(f"\n🌐 Création du CSV global consolidé...")
        
        all_dataframes = []
        
        for pdf_name, result in all_results.items():
            if result.get('merged_dataframe') is not None:
                df = result['merged_dataframe'].copy()
                all_dataframes.append(df)
                print(f"   📄 {pdf_name}: {len(df)} lignes ajoutées")
        
        if not all_dataframes:
            print("   ❌ Aucune donnée à consolider")
            return None
        
        try:
            # Concaténer tous les DataFrames
            global_df = DataFrameCombiner.concatenate_all_dataframes(all_dataframes)
            
            # Créer le CSV global
            csv_buffer = io.StringIO()
            global_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            global_csv_data = csv_buffer.getvalue().encode('utf-8-sig')
            
            print(f"   ✅ CSV global créé: {len(global_df)} lignes totales, {len(global_df.columns)} colonnes")
            
            return global_csv_data
            
        except Exception as e:
            print(f"   ❌ Erreur lors de la création du CSV global: {e}")
            return None
    
    @staticmethod
    def get_global_csv_filename() -> str:
        """
        Génère un nom de fichier pour le CSV global
        
        Returns:
            Nom de fichier avec timestamp
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"extraction_globale_consolidee_{timestamp}.csv"


class ExtractionConfig:
    """Configuration pour l'extraction"""
    
    def __init__(self, pdf_path: str, page_ranges_dict: Dict[str, List[str]], 
                 output_directory: str = "extracted_categories",
                 extraction_methods: List[str] = None,
                 cleaning_rules: Dict[str, Any] = None,
                 column_mapping: Dict[str, str] = None,
                 filters: Dict[str, Any] = None):
        
        self.pdf_path = pdf_path
        self.page_ranges_dict = page_ranges_dict
        self.output_directory = output_directory
        self.extraction_methods = extraction_methods or ["pdfplumber"]
        self.cleaning_rules = cleaning_rules or DEFAULT_CLEANING_RULES.copy()
        self.column_mapping = column_mapping or {}
        self.filters = filters or {}
