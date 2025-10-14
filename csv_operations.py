"""
Opérations de création et gestion des fichiers CSV
"""

import pandas as pd
import io
import os
import tempfile
from processors import CategoryProcessor, DictionaryExtractionConfig
from extractors import creer_dictionnaire_plages_mots_cles
from utils import calculate_coverage_info
from config import MOTS_CLES, DEFAULT_CLEANING_RULES


class DictionaryCSVProcessor:
    def __init__(self, config: DictionaryExtractionConfig):
        self.config = config
        self.category_processor = CategoryProcessor(config)
        os.makedirs(config.output_directory, exist_ok=True)
    
    def process_all_categories(self, pdf_filename: str) -> tuple:
        print(f"🟢 Début du traitement de {len(self.config.page_ranges_dict)} catégories")
        print(f"📁 Répertoire de sortie: {self.config.output_directory}")
        
        from utils import FileNameSanitizer
        from config import DICO_BORDEREAU
        
        base_name = os.path.splitext(pdf_filename)[0]
        safe_base_name = FileNameSanitizer.sanitize_filename(base_name)
        csv_filename = f"{safe_base_name}.csv"
        csv_filepath = os.path.join(self.config.output_directory, csv_filename)
        
        all_dataframes = []
        processing_results = {}
        success_count = 0
        
        for category_name, page_ranges in self.config.page_ranges_dict.items():
            print(f"\n🔍 Traitement de la catégorie: '{category_name}'")
            
            df = self.category_processor.process_category(category_name, page_ranges)
            
            if df is not None and not df.empty:
                df = self._process_dataframe_columns(df, category_name)
                df = self._add_metadata_columns(df, pdf_filename, category_name)
                df = self._clean_and_filter_data(df, category_name)
                
                all_dataframes.append(df)
                
                processing_results[category_name] = {
                    'success': True,
                    'category_label': DICO_BORDEREAU[category_name],
                    'rows': len(df),
                    'cols': len(df.columns)
                }
                success_count += 1
                
                print(f"    ✅ Préparé: {DICO_BORDEREAU[category_name]} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")
            else:
                print(f"    ❌ Échec pour la catégorie '{category_name}'")
                processing_results[category_name] = {'success': False, 'error': 'Aucun tableau trouvé'}
        
        return self._create_final_csv(all_dataframes, csv_filepath, csv_filename, processing_results, success_count)
    
    def _process_dataframe_columns(self, df, category_name):
        """Traiter les colonnes du DataFrame"""
        # Logique de traitement des colonnes vides et renommage
        mask_unnamed = [
            (c is None) or (isinstance(c, str) and (c.strip() == "" or c.lower().startswith("unnamed:")))
            for c in df.columns
        ]
        
        if True in mask_unnamed:
            new_cols = []
            compte = 0
            for i, c in enumerate(df.columns):
                if mask_unnamed[i]:
                    compte += 1
                    left_name = new_cols[i-1] if i > 0 else "col0"
                    first_val = df.iloc[0, i] if len(df) > 0 else ""
                    left_first_val = df.iloc[0, i-1] if len(df) > 0 else ""
                    new_cols[i-1] = f"{left_name}_{str(left_first_val).strip()}"
                    new_cols.append(f"{left_name}_{str(first_val).strip()}")
                else:
                    new_cols.append(str(c))
        
            if compte > 0:
                df = df.iloc[1:].reset_index(drop=True)
        
            df.columns = new_cols
        
        return self._clean_column_names(df)
    
    def _add_metadata_columns(self, df, pdf_filename, category_name):
        """Ajouter les colonnes de métadonnées"""
        from config import DICO_BORDEREAU
        
        document_name = os.path.splitext(pdf_filename)[0]
        df.insert(0, 'Document', document_name)
        
        category_label = DICO_BORDEREAU[category_name]
        df.insert(1, 'Catégorie', category_label)
        
        return self._standardize_name_column(df)
    
    def _clean_and_filter_data(self, df, category_name):
        """Nettoyer et filtrer les données"""
        # Traitement spécifique pour Bordereau A5
        if category_name == "Bordereau A5 n" and len(df.columns) > 5:
            try:
                mask = df.iloc[:, 5].str.lower().str.strip() == 'aucune candidature'
                df.loc[mask, df.columns[0]] = 'aucune candidature'
                df.loc[mask, df.columns[5]] = ''
            except:
                pass

        # Supprimer les lignes vides
        mask = (df.iloc[:, 0].astype(str).str.strip() != '')
        df = df[mask].reset_index(drop=True)
        
        return df
    
    def _clean_column_names(self, df):
        """Nettoie les noms de colonnes"""
        import re
        
        cleaned_columns = []
        for col in df.columns:
            col_str = str(col)
            cleaned_col = col_str.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            cleaned_col = re.sub(r'\s+', ' ', cleaned_col)
            cleaned_col = cleaned_col.strip()
            cleaned_columns.append(cleaned_col)
        
        df.columns = cleaned_columns
        return df
    
    def _standardize_name_column(self, df):
        """Standardise le nom de la colonne contenant les noms et prénoms"""
        import re
        
        name_patterns = [
            r'nom.*pr[eé]nom', r'pr[eé]nom.*nom', r'^nom$', r'nom',
            r'pr[eé]nom', r'identit[eé]', r'personne'
        ]
        
        for col in df.columns:
            col_lower = str(col).lower()
            for pattern in name_patterns:
                if re.search(pattern, col_lower):
                    df = df.rename(columns={col: 'Nom & Prénom'})
                    return df
        
        if len(df.columns) > 2:
            df = df.rename(columns={df.columns[2]: 'Nom & Prénom'})
        
        return df
    
    def _create_final_csv(self, all_dataframes, csv_filepath, csv_filename, processing_results, success_count):
        """Créer le fichier CSV final"""
        csv_data = None
        merged_df = None
        
        if all_dataframes:
            try:
                merged_df = self._concatenate_all_dataframes(all_dataframes)
                
                if merged_df is not None and not merged_df.empty:
                    csv_buffer = io.StringIO()
                    merged_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                    csv_data = csv_buffer.getvalue().encode('utf-8-sig')
                    
                    with open(csv_filepath, 'w', encoding='utf-8-sig', newline='') as f:
                        f.write(csv_buffer.getvalue())
                    
                    print(f"\n✅ Fichier CSV créé avec succès: {csv_filename}")
                    print(f"📊 {len(merged_df)} lignes totales, {len(merged_df.columns)} colonnes")
                    print(f"\n📊 RÉSUMÉ:")
                    print(f"   ✅ {success_count}/{len(self.config.page_ranges_dict)} catégories traitées avec succès")
                    print(f"   📁 Fichier CSV: {csv_filepath}")
                else:
                    print("❌ DataFrame fusionné vide ou None")
                    return None, {}, 0, None, None
                    
            except Exception as e:
                print(f"❌ Erreur lors de la création du fichier CSV: {e}")
                return None, {}, 0, None, None
        else:
            print("❌ Aucune donnée à écrire dans le fichier CSV")
            return None, {}, 0, None, None
        
        return csv_filepath, processing_results, success_count, csv_data, merged_df
    
    def _concatenate_all_dataframes(self, dataframes_list):
        """Concatène tous les DataFrames"""
        if not dataframes_list:
            return pd.DataFrame()
            
        if len(dataframes_list) == 1:
            return dataframes_list[0]
        
        try:
            print(f"🔗 Concaténation de {len(dataframes_list)} DataFrames...")
            
            clean_dataframes = []
            for i, df in enumerate(dataframes_list):
                if df is not None and not df.empty:
                    clean_df = df.copy().reset_index(drop=True)
                    
                    if clean_df.columns.duplicated().any():
                        print(f"   ⚠️ Colonnes dupliquées dans DataFrame {i+1}")
                        cols = clean_df.columns.tolist()
                        for j, col in enumerate(cols):
                            if cols.count(col) > 1:
                                cols[j] = f"{col}_{j}"
                        clean_df.columns = cols
                    
                    clean_dataframes.append(clean_df)
                    print(f"   DataFrame {i+1}: {len(clean_df)} lignes préparées")
            
            if not clean_dataframes:
                return pd.DataFrame()
            
            merged_df = pd.concat(clean_dataframes, ignore_index=True, sort=False)
            
            # Réorganiser les colonnes
            cols_to_front = []
            if 'Document' in merged_df.columns:
                cols_to_front.append('Document')
            if 'Catégorie' in merged_df.columns:
                cols_to_front.append('Catégorie')
            if 'Nom & Prénom' in merged_df.columns:
                cols_to_front.append('Nom & Prénom')
            
            remaining_cols = [col for col in merged_df.columns if col not in cols_to_front]
            final_columns_order = cols_to_front + remaining_cols
            
            merged_df = merged_df[final_columns_order]
            merged_df = merged_df.fillna('')
            
            print(f"   ✅ Concaténation réussie: {len(merged_df)} lignes totales")
            
            return merged_df
            
        except Exception as e:
            print(f"❌ Erreur concaténation: {e}")
            if dataframes_list:
                largest = max(dataframes_list, key=lambda x: len(x) if x is not None else 0)
                return largest.reset_index(drop=True) if largest is not None else pd.DataFrame()
            return pd.DataFrame()


def process_single_pdf(pdf_path, pdf_filename, temp_dir):
    """Traiter un seul PDF"""
    print(f"\n{'='*60}")
    print(f"🔍 TRAITEMENT: {pdf_filename}")
    print(f"{'='*60}")
    
    # Analyser le PDF
    dictionnaire_plages = creer_dictionnaire_plages_mots_cles(
        pdf_path, MOTS_CLES, ignorer_casse=True
    )
    
    # Calculer la couverture
    coverage_info = calculate_coverage_info(pdf_path, dictionnaire_plages)
    
    # Traitement CSV
    config = DictionaryExtractionConfig(
        pdf_path=pdf_path,
        page_ranges_dict=dictionnaire_plages,
        output_directory=temp_dir,
        cleaning_rules=DEFAULT_CLEANING_RULES
    )
    
    processor = DictionaryCSVProcessor(config)
    csv_filepath, processing_results, success_count, csv_data, merged_df = processor.process_all_categories(pdf_filename)
    
    return {
        'pdf_filename': pdf_filename,
        'csv_filepath': csv_filepath,
        'processing_results': processing_results,
        'success_count': success_count,
        'csv_data': csv_data,
        'coverage_info': coverage_info,
        'dictionnaire_plages': dictionnaire_plages,
        'merged_dataframe': merged_df
    }


def create_global_csv(all_results):
    """Créer un CSV global consolidant toutes les données de tous les PDF"""
    print(f"\n🌐 Création du CSV global consolidé...")
    
    all_global_dataframes = []
    
    for pdf_name, result in all_results.items():
        if result.get('merged_dataframe') is not None:
            df = result['merged_dataframe'].copy()
            all_global_dataframes.append(df)
            print(f"   📄 {pdf_name}: {len(df)} lignes ajoutées")
    
    if not all_global_dataframes:
        print("   ❌ Aucune donnée à consolider")
        return None
    
    try:
        clean_global_dataframes = []
        for i, df in enumerate(all_global_dataframes):
            if df is not None and not df.empty:
                clean_df = df.copy().reset_index(drop=True)
                clean_global_dataframes.append(clean_df)
        
        if not clean_global_dataframes:
            return None
        
        global_df = pd.concat(clean_global_dataframes, ignore_index=True, sort=False)
        
        # Réorganiser les colonnes
        cols_to_front = []
        if 'Document' in global_df.columns:
            cols_to_front.append('Document')
        if 'Catégorie' in global_df.columns:
            cols_to_front.append('Catégorie')
        if 'Nom & Prénom' in global_df.columns:
            cols_to_front.append('Nom & Prénom')
        
        remaining_cols = [col for col in global_df.columns if col not in cols_to_front]
        final_columns_order = cols_to_front + remaining_cols
        
        global_df = global_df[final_columns_order]
        global_df = global_df.fillna('')
        
        # Créer le CSV global
        csv_buffer = io.StringIO()
        global_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
        global_csv_data = csv_buffer.getvalue().encode('utf-8-sig')
        
        print(f"   ✅ CSV global créé: {len(global_df)} lignes totales, {len(global_df.columns)} colonnes")
        
        return global_csv_data
        
    except Exception as e:
        print(f"   ❌ Erreur lors de la création du CSV global: {e}")
        return None
