"""
Point d'entrée principal de l'application Extracteur Multi-PDF vers CSV Global
"""

import streamlit as st
import tempfile
import os
from src.streamlit_ui import StreamlitUI
from src.pdf_analyzer import PDFAnalyzer, CoverageAnalyzer
from src.csv_generator import CSVGenerator, GlobalCSVGenerator, ExtractionConfig
from src.utils import OutputCapture
from config import STREAMLIT_CONFIG, MOTS_CLES, DEFAULT_CLEANING_RULES


class PDFExtractorApp:
    """Application principale pour l'extraction PDF"""
    
    def __init__(self):
        self.ui = StreamlitUI()
        self.analyzer = PDFAnalyzer(ignore_case=True)
        self.coverage_analyzer = CoverageAnalyzer()
        
        # Configuration Streamlit
        st.set_page_config(**STREAMLIT_CONFIG)
    
    def run(self):
        """Lance l'application"""
        self.ui.render_main_interface()
        
        # Vérifier si des fichiers ont été uploadés et si le bouton a été cliqué
        if 'uploaded_files' in st.session_state and st.session_state.uploaded_files:
            if st.session_state.get('start_processing', False):
                # Réinitialiser le flag
                st.session_state.start_processing = False
                self._process_uploaded_files(st.session_state.uploaded_files)
    
    def _process_uploaded_files(self, uploaded_files):
        """Traite les fichiers uploadés"""
        with st.spinner(f"🔍 Traitement de {len(uploaded_files)} fichier(s) PDF en cours..."):
            
            # Initialiser le suivi
            all_logs = []
            all_results = {}
            total_success = 0
            
            # Barre de progression
            progress_bar, status_text = self.ui.show_processing_progress(len(uploaded_files))
            
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    # Traiter chaque PDF
                    for i, uploaded_file in enumerate(uploaded_files):
                        self.ui.update_progress(progress_bar, status_text, i, len(uploaded_files), uploaded_file.name)
                        
                        result = self._process_single_pdf(uploaded_file, temp_dir)
                        
                        # Capturer les logs
                        all_logs.append(f"\n{'='*60}")
                        all_logs.append(f"PDF: {uploaded_file.name}")
                        all_logs.append(f"{'='*60}")
                        
                        if 'log' in result:
                            all_logs.append(result['log'])
                        
                        # Stocker le résultat
                        all_results[uploaded_file.name] = result
                        
                        if result.get('csv_data'):
                            total_success += 1
                    
                    # Créer le CSV global
                    global_csv_data = self._create_global_csv(all_results)
                    all_logs.append(f"\n{'='*60}")
                    all_logs.append("CONSOLIDATION GLOBALE")
                    all_logs.append(f"{'='*60}")
                    
                    # Finaliser
                    self.ui.finalize_progress(progress_bar, status_text)
                    
                    # Sauvegarder les résultats
                    self._save_results(all_results, all_logs, total_success, len(uploaded_files), global_csv_data)
                    
                    # Message de succès
                    self.ui.show_success_message(total_success, len(uploaded_files), global_csv_data is not None)
                    
                    # Nettoyer les fichiers uploadés du session state
                    if 'uploaded_files' in st.session_state:
                        del st.session_state.uploaded_files
                    
                    # Recharger pour afficher les résultats
                    st.rerun()
            
            except Exception as e:
                self.ui.show_error_message(e)
                # En cas d'erreur, aussi nettoyer
                if 'uploaded_files' in st.session_state:
                    del st.session_state.uploaded_files

    
    def _process_single_pdf(self, uploaded_file, temp_dir):
        """Traite un seul PDF"""
        # Créer fichier temporaire
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            temp_pdf_path = tmp_file.name
        
        try:
            def run_extraction():
                return self._extract_pdf_data(temp_pdf_path, uploaded_file.name, temp_dir)
            
            result, output = OutputCapture.capture_prints(run_extraction)
            result['log'] = output
            return result
            
        except Exception as e:
            return {
                'pdf_filename': uploaded_file.name,
                'csv_filepath': None,
                'processing_results': {},
                'success_count': 0,
                'csv_data': None,
                'coverage_info': {},
                'dictionnaire_plages': {},
                'merged_dataframe': None,
                'log': f"❌ ERREUR: {e}"
            }
        finally:
            # Nettoyer le fichier temporaire
            if os.path.exists(temp_pdf_path):
                os.unlink(temp_pdf_path)
    
    def _extract_pdf_data(self, pdf_path, pdf_filename, temp_dir):
        """Extrait les données d'un PDF"""
        print(f"\n{'='*60}")
        print(f"🔍 TRAITEMENT: {pdf_filename}")
        print(f"{'='*60}")
        
        # Analyser le PDF
        page_ranges_dict = self.analyzer.analyze_pdf(pdf_path, MOTS_CLES)
        
        # Calculer la couverture
        coverage_info = self.coverage_analyzer.calculate_coverage(pdf_path, page_ranges_dict)
        
        # Configuration de l'extraction
        config = ExtractionConfig(
            pdf_path=pdf_path,
            page_ranges_dict=page_ranges_dict,
            output_directory=temp_dir,
            cleaning_rules=DEFAULT_CLEANING_RULES.copy()
        )
        
        # Générer le CSV
        generator = CSVGenerator(config)
        csv_filepath, processing_results, success_count, csv_data, merged_df = generator.generate_individual_csv(pdf_filename)
        
        return {
            'pdf_filename': pdf_filename,
            'csv_filepath': csv_filepath,
            'processing_results': processing_results,
            'success_count': success_count,
            'csv_data': csv_data,
            'coverage_info': coverage_info,
            'dictionnaire_plages': page_ranges_dict,
            'merged_dataframe': merged_df
        }
    
    def _create_global_csv(self, all_results):
        """Crée le CSV global consolidé"""
        def run_global_creation():
            return GlobalCSVGenerator.create_global_csv(all_results)
        
        global_csv_data, global_output = OutputCapture.capture_prints(run_global_creation)
        return global_csv_data
    
    def _save_results(self, all_results, all_logs, total_success, total_processed, global_csv_data):
        """Sauvegarde les résultats dans l'état de session"""
        st.session_state.all_results = all_results
        st.session_state.global_csv_data = global_csv_data
        st.session_state.output_log = "\n".join(all_logs)
        st.session_state.total_processed = total_processed
        st.session_state.total_success = total_success
        st.session_state.extraction_done = True


def main():
    """Fonction principale"""
    app = PDFExtractorApp()
    app.run()


if __name__ == "__main__":
    main()
