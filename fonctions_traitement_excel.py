import os
import re
import io
import pandas as pd
from config import dico_bordereau
from fonctions_traitement_dataframes import CategoryProcessor

class FileNameSanitizer:
    @staticmethod
    def sanitize_filename(name: str) -> str:
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
        sanitized = re.sub(r'\s+', '_', sanitized)
        sanitized = sanitized.strip('._-')
        sanitized = sanitized[:50] if len(sanitized) > 50 else sanitized
        return sanitized

class ExcelSheetNameSanitizer:
    @staticmethod
    def sanitize_sheet_name(name: str) -> str:
        sanitized = re.sub(r'[\\\/\?\*\[\]:]', '_', name)
        sanitized = sanitized.strip()
        if len(sanitized) > 31:
            sanitized = sanitized[:28] + "..."
        return sanitized

class DictionaryExcelProcessor:
    def __init__(self, config):
        self.config = config
        self.category_processor = CategoryProcessor(config)
        os.makedirs(config.output_directory, exist_ok=True)
    
    def process_all_categories(self, pdf_filename: str) -> tuple:
        print(f"🟢 Début du traitement de {len(self.config.page_ranges_dict)} catégories")
        print(f"📁 Répertoire de sortie: {self.config.output_directory}")
        
        # Utiliser le nom du PDF pour le fichier Excel
        base_name = os.path.splitext(pdf_filename)[0]
        safe_base_name = FileNameSanitizer.sanitize_filename(base_name)
        excel_filename = f"{safe_base_name}.xlsx"
        excel_filepath = os.path.join(self.config.output_directory, excel_filename)
        
        sheets_data = {}
        processing_results = {}
        success_count = 0
        
        for category_name, page_ranges in self.config.page_ranges_dict.items():
            print(f"\n🔍 Traitement de la catégorie: '{category_name}'")
            
            df = self.category_processor.process_category(category_name, page_ranges)
            
            if df is not None and not df.empty:
                if len(df.columns) >= 6:
                    mask = (df.index == 0) | ((df.iloc[:, 0].astype(str).str.strip() != '') & (df.iloc[:, 5] != "AUCUNE CANDIDATURE"))
                else:
                    mask = (df.index == 0) | ((df.iloc[:, 0].astype(str).str.strip() != ''))
                df = df[mask].reset_index(drop=True)
                
                sheet_name = ExcelSheetNameSanitizer.sanitize_sheet_name(dico_bordereau[category_name])
                sheets_data[sheet_name] = df
                processing_results[category_name] = {
                    'success': True,
                    'sheet_name': sheet_name,
                    'rows': len(df),
                    'cols': len(df.columns)
                }
                success_count += 1
                
                print(f"    ✅ Préparé: {sheet_name} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")
            else:
                print(f"    ❌ Échec pour la catégorie '{category_name}'")
                processing_results[category_name] = {'success': False, 'error': 'Aucun tableau trouvé'}
        
        excel_data = None
        if sheets_data:
            try:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    for sheet_name, df in sheets_data.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                excel_data = excel_buffer.getvalue()
                
                with open(excel_filepath, 'wb') as f:
                    f.write(excel_data)
                
                print(f"\n✅ Fichier Excel créé avec succès: {excel_filename}")
                print(f"📊 {len(sheets_data)} feuilles créées")
                
                success_count = len(sheets_data)
                total_count = len(self.config.page_ranges_dict)
                print(f"\n📊 RÉSUMÉ:")
                print(f"   ✅ {success_count}/{total_count} catégories traitées avec succès")
                print(f"   📁 Fichier Excel: {excel_filepath}")
                
            except Exception as e:
                print(f"❌ Erreur lors de la création du fichier Excel: {e}")
                return None, {}, 0, None
        else:
            print("❌ Aucune donnée à écrire dans le fichier Excel")
            return None, {}, 0, None
        
        return excel_filepath, processing_results, success_count, excel_data
