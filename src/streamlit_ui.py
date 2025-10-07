"""
Interface utilisateur Streamlit
"""

import os
import streamlit as st
import pandas as pd
import io
import zipfile
from datetime import datetime
from typing import Dict, Any
from .utils import OutputCapture, FileNameSanitizer, format_page_ranges


class StreamlitUI:
    """Classe pour gérer l'interface utilisateur Streamlit"""
    
    def __init__(self):
        self._init_session_state()
    
    def _init_session_state(self):
        """Initialise l'état de session Streamlit"""
        if 'extraction_done' not in st.session_state:
            st.session_state.extraction_done = False
        if 'all_results' not in st.session_state:
            st.session_state.all_results = {}
        if 'global_csv_data' not in st.session_state:
            st.session_state.global_csv_data = None
        if 'output_log' not in st.session_state:
            st.session_state.output_log = ""
        if 'total_processed' not in st.session_state:
            st.session_state.total_processed = 0
        if 'total_success' not in st.session_state:
            st.session_state.total_success = 0
    
    def render_main_interface(self):
        """Rend l'interface principale"""
        st.title("🌐 Extracteur Multi-PDF vers CSV Global")
        st.markdown("---")
        
        # Si extraction terminée, afficher les résultats
        if st.session_state.extraction_done:
            self.show_results()
            return
        
        # Sinon, afficher l'interface d'upload
        self.show_upload_interface()
    
    def show_upload_interface(self):
        """Affiche l'interface d'upload et de traitement"""
        # Description
        st.markdown("""
        Cette application permet d'extraire des tableaux de **plusieurs fichiers PDF** et de les convertir en :
        
        1. **CSV individuels** : un fichier par PDF (avec colonne "Document")
        2. **CSV global consolidé** : un fichier unique contenant toutes les données de tous les PDF
        
        **Comment l'utiliser :**
        1. 📤 Uploadez vos fichiers PDF (plusieurs acceptés)
        2. 🚀 Cliquez sur "Lancer l'extraction de tous les PDF"
        3. 📥 Téléchargez le CSV global ou les CSV individuels
        
        **Structure du CSV global :**
        - 🏷️ **Colonne 1** : "Document" (nom du PDF d'origine)
        - 🏷️ **Colonne 2** : "Catégorie" (type de bordereau)
        - 👤 **Colonne 3** : "Nom & Prénom"
        - 📊 **Autres colonnes** : toutes les données spécifiques
        """)
        
        # Upload des fichiers
        st.subheader("📤 Upload des fichiers PDF")
        uploaded_files = st.file_uploader(
            "Choisissez vos fichiers PDF", 
            type="pdf",
            accept_multiple_files=True,
            help="Vous pouvez sélectionner plusieurs fichiers PDF à traiter"
        )
        
        if uploaded_files:
            self._show_file_info(uploaded_files)
            
            # Bouton de traitement
            if st.button("🚀 Lancer l'extraction de tous les PDF", type="primary"):
                return uploaded_files
        else:
            st.info("👆 Veuillez uploader un ou plusieurs fichiers PDF pour commencer")
        
        return None
    
    def _show_file_info(self, uploaded_files):
        """Affiche les informations des fichiers uploadés"""
        st.success(f"✅ {len(uploaded_files)} fichier(s) uploadé(s)")
        
        st.write("**📂 Fichiers sélectionnés :**")
        
        total_size = 0
        for i, file in enumerate(uploaded_files, 1):
            size = len(file.getvalue())
            total_size += size
            
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"**{i}.** {file.name}")
            with col2:
                st.write(f"{size:,} bytes")
        
        st.info(f"📊 Total : {len(uploaded_files)} fichier(s) - {total_size:,} bytes")
    
    def show_processing_progress(self, total_files: int):
        """Affiche la barre de progression pendant le traitement"""
        progress_bar = st.progress(0)
        status_text = st.empty()
        return progress_bar, status_text
    
    def update_progress(self, progress_bar, status_text, current: int, 
                       total: int, current_file: str):
        """Met à jour la barre de progression"""
        progress = current / total
        progress_bar.progress(progress)
        status_text.text(f"Traitement de {current_file} ({current+1}/{total})")
    
    def finalize_progress(self, progress_bar, status_text):
        """Finalise la barre de progression"""
        progress_bar.progress(1.0)
        status_text.text("✅ Traitement terminé!")
    
    def show_results(self):
        """Affiche les résultats de tous les PDF"""
        if not (st.session_state.extraction_done and st.session_state.all_results):
            return
        
        # Bouton pour refaire une extraction
        if st.button("🔄 Nouvelle extraction", type="secondary"):
            self.reset_extraction()
            st.rerun()
        
        st.markdown("---")
        
        # Afficher les sections
        self._show_logs_section()
        self._show_overview_section()
        self._show_global_csv_section()
        self._show_individual_results_section()
        self._show_download_section()
    
    def _show_logs_section(self):
        """Affiche la section des logs"""
        st.subheader("📋 Console du programme")
        with st.expander("Voir les logs détaillés", expanded=False):
            st.code(st.session_state.output_log, language="text")
    
    def _show_overview_section(self):
        """Affiche la vue d'ensemble"""
        st.subheader("📊 Vue d'ensemble")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📄 PDF traités", st.session_state.total_processed)
        with col2:
            st.metric("✅ Extractions réussies", st.session_state.total_success)
        with col3:
            failed = st.session_state.total_processed - st.session_state.total_success
            st.metric("❌ Échecs", failed)
    
    def _show_global_csv_section(self):
        """Affiche la section du CSV global"""
        st.markdown("---")
        st.subheader("🌐 CSV Global Consolidé")
        
        if st.session_state.global_csv_data:
            try:
                csv_content = st.session_state.global_csv_data.decode('utf-8-sig')
                global_preview_df = pd.read_csv(io.StringIO(csv_content))
                
                self._render_global_csv_info(global_preview_df)
                self._render_global_csv_preview(global_preview_df)
                self._render_global_csv_statistics(global_preview_df)
                
            except Exception as e:
                st.error(f"Erreur lors de l'affichage du CSV global: {e}")
        else:
            st.warning("❌ Aucun CSV global n'a pu être créé")
    
    def _render_global_csv_info(self, global_df):
        """Rend les informations du CSV global"""
        col1, col2 = st.columns(2)
        with col1:
            st.success("✅ CSV global créé avec succès !")
            st.info(f"📊 **{len(global_df)} lignes totales** de tous les PDF")
            st.info(f"📋 **{len(global_df.columns)} colonnes** consolidées")
        with col2:
            # Bouton de téléchargement du CSV global
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            st.download_button(
                label="🌐 Télécharger le CSV Global Consolidé",
                data=st.session_state.global_csv_data,
                file_name=f"extraction_globale_consolidee_{timestamp}.csv",
                mime="text/csv",
                key="download_global_csv",
                use_container_width=True,
                type="primary"
            )
    
    def _render_global_csv_preview(self, global_df):
        """Rend l'aperçu du CSV global"""
        st.write("**👀 Aperçu du CSV Global**")
        st.dataframe(global_df.head(15), use_container_width=True)
    
    def _render_global_csv_statistics(self, global_df):
        """Rend les statistiques du CSV global"""
        if len(global_df) == 0:
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Répartition par document
            if 'Document' in global_df.columns:
                doc_counts = global_df['Document'].value_counts()
                st.write("**📄 Répartition par Document :**")
                st.bar_chart(doc_counts)
        
        with col2:
            # Répartition par catégorie
            if 'Catégorie' in global_df.columns:
                category_counts = global_df['Catégorie'].value_counts()
                st.write("**📈 Répartition par Catégorie :**")
                st.bar_chart(category_counts)
        
        # Tableau croisé dynamique
        if 'Document' in global_df.columns and 'Catégorie' in global_df.columns:
            st.write("**📊 Tableau croisé : Documents vs Catégories**")
            cross_tab = pd.crosstab(global_df['Document'], global_df['Catégorie'])
            st.dataframe(cross_tab, use_container_width=True)
    
    def _show_individual_results_section(self):
        """Affiche la section des résultats individuels"""
        st.markdown("---")
        st.subheader("📂 Résultats individuels par PDF")
        
        for pdf_name, result in st.session_state.all_results.items():
            with st.expander(f"📄 {pdf_name}", expanded=False):
                self._render_individual_pdf_results(pdf_name, result)
    
    def _render_individual_pdf_results(self, pdf_name: str, result: Dict[str, Any]):
        """Rend les résultats d'un PDF individuel"""
        # Statut général
        if result['csv_data']:
            st.success(f"✅ Traitement réussi - {result['success_count']} catégories extraites")
        else:
            st.error("❌ Échec du traitement")
        
        col1, col2 = st.columns(2)
        
        # Informations de couverture
        with col1:
            self._render_coverage_info(result['coverage_info'])
        
        # Détails des catégories
        with col2:
            self._render_category_details(result['processing_results'])
        
        # Téléchargement et aperçu
        if result['csv_data']:
            self._render_individual_download(pdf_name, result)
    
    def _render_coverage_info(self, coverage: Dict):
        """Rend les informations de couverture"""
        st.write("**📑 Couverture du document**")
        
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            st.metric("Pages totales", coverage['total_pages'])
            st.metric("Pages traitées", coverage['nb_pages_traitees'])
        with subcol2:
            st.metric("Pages non traitées", coverage['nb_pages_non_traitees'])
            st.metric("Taux de couverture", f"{coverage['pourcentage_couverture']}%")
        
        # Barre de progression
        st.progress(coverage['pourcentage_couverture'] / 100)
        
        # Pages non traitées
        if coverage['nb_pages_non_traitees'] > 0:
            formatted_pages = format_page_ranges(coverage['pages_non_traitees'])
            st.warning(f"**Pages non traitées :** {formatted_pages}")
        else:
            st.success("🎉 **Toutes les pages ont été traitées !**")
    
    def _render_category_details(self, processing_results: Dict):
        """Rend les détails des catégories"""
        st.write("**📋 Détail des catégories**")
        
        if processing_results:
            recap_data = []
            for category, data_info in processing_results.items():
                if data_info.get('success'):
                    recap_data.append({
                        'Catégorie': category,
                        'Statut': "✅ Succès",
                        'Lignes': data_info['rows'],
                        'Colonnes': data_info['cols']
                    })
                else:
                    recap_data.append({
                        'Catégorie': category,
                        'Statut': "❌ Échec",
                        'Lignes': 0,
                        'Colonnes': 0
                    })
            
            if recap_data:
                recap_df = pd.DataFrame(recap_data)
                st.dataframe(recap_df, use_container_width=True, height=200)
    
    def _render_individual_download(self, pdf_name: str, result: Dict):
        """Rend la section de téléchargement individuel"""
        base_name = os.path.splitext(pdf_name)[0]
        safe_base_name = FileNameSanitizer.sanitize_filename(base_name)
        
        st.download_button(
            label=f"📊 Télécharger {pdf_name}.csv",
            data=result['csv_data'],
            file_name=f"{safe_base_name}.csv",
            mime="text/csv",
            key=f"download_{pdf_name}",
            use_container_width=True
        )
        
        # Aperçu des données
        with st.expander(f"👀 Aperçu des données de {pdf_name}"):
            try:
                csv_content = result['csv_data'].decode('utf-8-sig')
                preview_df = pd.read_csv(io.StringIO(csv_content))
                st.info(f"📊 {len(preview_df)} lignes, {len(preview_df.columns)} colonnes")
                st.dataframe(preview_df.head(5), use_container_width=True)
            except Exception as e:
                st.error(f"Erreur lors de la lecture du CSV: {e}")
    
    def _show_download_section(self):
        """Affiche la section de téléchargement groupé"""
        st.markdown("---")
        st.subheader("📦 Téléchargement groupé des CSV individuels")
        
        successful_csvs = {
            name: result for name, result in st.session_state.all_results.items() 
            if result['csv_data'] is not None
        }
        
        if len(successful_csvs) > 1:
            self._render_zip_download(successful_csvs)
        elif len(successful_csvs) == 1:
            st.info("📊 Un seul fichier CSV généré - utilisez le téléchargement individuel ci-dessus")
        else:
            st.warning("❌ Aucun fichier CSV généré avec succès")
    
    def _render_zip_download(self, successful_csvs: Dict):
        """Rend la section de téléchargement ZIP"""
        # Créer le fichier ZIP
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for pdf_name, result in successful_csvs.items():
                if result['csv_data']:
                    base_name = os.path.splitext(pdf_name)[0]
                    safe_base_name = FileNameSanitizer.sanitize_filename(base_name)
                    csv_filename = f"{safe_base_name}.csv"
                    zip_file.writestr(csv_filename, result['csv_data'])
        
        zip_buffer.seek(0)
        
        col1, col2 = st.columns([2, 1])
        with col1:
            st.info(f"📊 {len(successful_csvs)} fichiers CSV individuels prêts à télécharger")
        with col2:
            st.metric("📦 Fichiers dans le ZIP", len(successful_csvs))
        
        # Bouton de téléchargement du ZIP
        st.download_button(
            label="📦 Télécharger tous les CSV individuels (ZIP)",
            data=zip_buffer.getvalue(),
            file_name=f"extraction_csv_individuels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            mime="application/zip",
            key="download_all_csv_zip",
            use_container_width=True
        )
    
    def reset_extraction(self):
        """Remet à zéro l'extraction"""
        st.session_state.extraction_done = False
        st.session_state.all_results = {}
        st.session_state.global_csv_data = None
        st.session_state.output_log = ""
        st.session_state.total_processed = 0
        st.session_state.total_success = 0
    
    def show_success_message(self, total_success: int, total_processed: int, 
                           has_global_csv: bool):
        """Affiche le message de succès final"""
        success_msg = f"🎉 Traitement terminé ! {total_success}/{total_processed} PDF traités avec succès"
        if has_global_csv:
            success_msg += "\n🌐 CSV global consolidé créé avec succès !"
        st.success(success_msg)
    
    def show_error_message(self, error: Exception):
        """Affiche un message d'erreur"""
        st.error(f"❌ Erreur générale lors du traitement: {error}")
        st.exception(error)
