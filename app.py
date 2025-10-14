"""
Interface Streamlit pour l'application d'extraction PDF vers CSV
"""

import streamlit as st
import pandas as pd
import os
import io
import tempfile
import zipfile
from datetime import datetime

from config import STREAMLIT_CONFIG
from csv_operations import process_single_pdf, create_global_csv
from utils import capture_prints, FileNameSanitizer

# Configuration de la page Streamlit
st.set_page_config(**STREAMLIT_CONFIG)

# Initialiser le session state
def init_session_state():
    session_vars = {
        'extraction_done': False,
        'all_results': {},
        'global_csv_data': None,
        'output_log': "",
        'total_processed': 0,
        'total_success': 0
    }
    
    for var, default_value in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default_value

def reset_extraction():
    """Remettre à zéro l'extraction"""
    st.session_state.extraction_done = False
    st.session_state.all_results = {}
    st.session_state.global_csv_data = None
    st.session_state.output_log = ""
    st.session_state.total_processed = 0
    st.session_state.total_success = 0

def show_results():
    """Afficher les résultats de tous les PDF"""
    if st.session_state.extraction_done and st.session_state.all_results:
        
        # Bouton pour refaire une extraction
        if st.button("🔄 Nouvelle extraction", type="secondary"):
            reset_extraction()
            st.rerun()
        
        st.markdown("---")
        
        # Afficher les logs
        st.subheader("📋 Console du programme")
        with st.expander("Voir les logs détaillés", expanded=False):
            st.code(st.session_state.output_log, language="text")
        
        # Vue d'ensemble
        st.subheader("📊 Vue d'ensemble")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📄 PDF traités", st.session_state.total_processed)
        with col2:
            st.metric("✅ Extractions réussies", st.session_state.total_success)
        with col3:
            failed = st.session_state.total_processed - st.session_state.total_success
            st.metric("❌ Échecs", failed)
        
        # Section CSV Global consolidé
        show_global_csv_section()
        
        # Résultats par PDF
        show_individual_results()
        
        # Téléchargement ZIP
        show_zip_download_section()

def show_global_csv_section():
    """Afficher la section CSV Global consolidé"""
    st.markdown("---")
    st.subheader("🌐 CSV Global Consolidé")
    
    if st.session_state.global_csv_data:
        try:
            csv_content = st.session_state.global_csv_data.decode('utf-8-sig')
            global_preview_df = pd.read_csv(io.StringIO(csv_content))
            
            col1, col2 = st.columns(2)
            with col1:
                st.success(f"✅ CSV global créé avec succès !")
                st.info(f"📊 **{len(global_preview_df)} lignes totales** de tous les PDF")
                st.info(f"📋 **{len(global_preview_df.columns)} colonnes** consolidées")
            with col2:
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
            
            # Aperçu et statistiques
            show_global_csv_preview(global_preview_df)
                    
        except Exception as e:
            st.error(f"Erreur lors de l'affichage du CSV global: {e}")
    else:
        st.warning("❌ Aucun CSV global n'a pu être créé")

def show_global_csv_preview(global_preview_df):
    """Afficher l'aperçu du CSV global"""
    st.write("**👀 Aperçu du CSV Global**")
    st.dataframe(global_preview_df.head(15), use_container_width=True)
    
    if len(global_preview_df) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            if 'Document' in global_preview_df.columns:
                doc_counts = global_preview_df['Document'].value_counts()
                st.write("**📄 Répartition par Document :**")
                st.bar_chart(doc_counts)
        
        with col2:
            if 'Catégorie' in global_preview_df.columns:
                category_counts = global_preview_df['Catégorie'].value_counts()
                st.write("**📈 Répartition par Catégorie :**")
                st.bar_chart(category_counts)
        
        # Tableau croisé dynamique
        if 'Document' in global_preview_df.columns and 'Catégorie' in global_preview_df.columns:
            st.write("**📊 Tableau croisé : Documents vs Catégories**")
            cross_tab = pd.crosstab(global_preview_df['Document'], global_preview_df['Catégorie'])
            st.dataframe(cross_tab, use_container_width=True)

def show_individual_results():
    """Afficher les résultats individuels par PDF"""
    st.markdown("---")
    st.subheader("📂 Résultats individuels par PDF")
    
    for pdf_name, result in st.session_state.all_results.items():
        with st.expander(f"📄 {pdf_name}", expanded=False):
            show_pdf_result_details(pdf_name, result)

def show_pdf_result_details(pdf_name, result):
    """Afficher les détails d'un résultat PDF"""
    # Statut général
    if result['csv_data']:
        st.success(f"✅ Traitement réussi - {result['success_count']} catégories extraites")
    else:
        st.error("❌ Échec du traitement")
    
    col1, col2 = st.columns(2)
    
    # Informations de couverture
    with col1:
        show_coverage_info(result['coverage_info'])
    
    # Détails des catégories
    with col2:
        show_category_details(result['processing_results'])
    
    # Téléchargement et aperçu
    show_individual_download_and_preview(pdf_name, result)

def show_coverage_info(coverage):
    """Afficher les informations de couverture"""
    st.write("**📑 Couverture du document**")
    
    subcol1, subcol2 = st.columns(2)
    with subcol1:
        st.metric("Pages totales", coverage['total_pages'])
        st.metric("Pages traitées", coverage['nb_pages_traitees'])
    with subcol2:
        st.metric("Pages non traitées", coverage['nb_pages_non_traitees'])
        st.metric("Taux de couverture", f"{coverage['pourcentage_couverture']}%")
    
    st.progress(coverage['pourcentage_couverture'] / 100)

def show_category_details(processing_results):
    """Afficher les détails des catégories"""
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

def show_individual_download_and_preview(pdf_name, result):
    """Afficher le téléchargement et aperçu individuel"""
    if result['csv_data']:
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
        
        # Aperçu
        with st.expander(f"👀 Aperçu des données de {pdf_name}"):
            try:
                csv_content = result['csv_data'].decode('utf-8-sig')
                preview_df = pd.read_csv(io.StringIO(csv_content))
                st.info(f"📊 {len(preview_df)} lignes, {len(preview_df.columns)} colonnes")
                st.dataframe(preview_df.head(5), use_container_width=True)
                
                if 'Nom & Prénom' in preview_df.columns:
                    duplicate_names = preview_df[preview_df.duplicated(subset=['Nom & Prénom'], keep=False)]
                    if not duplicate_names.empty:
                        st.info(f"🔄 {len(duplicate_names)} lignes avec des noms en double (personnes dans plusieurs catégories)")
            except Exception as e:
                st.error(f"Erreur lors de la lecture du CSV: {e}")

def show_zip_download_section():
    """Afficher la section de téléchargement ZIP"""
    st.markdown("---")
    st.subheader("📦 Téléchargement groupé des CSV individuels")
    
    successful_csvs = {name: result for name, result in st.session_state.all_results.items() 
                      if result['csv_data'] is not None}
    
    if len(successful_csvs) > 1:
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
        
        st.download_button(
            label=f"📦 Télécharger tous les CSV individuels (ZIP)",
            data=zip_buffer.getvalue(),
            file_name=f"extraction_csv_individuels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            mime="application/zip",
            key="download_all_csv_zip",
            use_container_width=True
        )
            
    elif len(successful_csvs) == 1:
        st.info("📊 Un seul fichier CSV généré - utilisez le téléchargement individuel ci-dessus")
    else:
        st.warning("❌ Aucun fichier CSV généré avec succès")

def main():
    """Interface principale"""
    init_session_state()
    
    st.title("🌐 Extracteur Multi-PDF vers CSV Global")
    st.markdown("---")
    
    # Si extraction terminée, afficher les résultats
    if st.session_state.extraction_done:
        show_results()
        return
    
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
    
    # Upload et traitement
    handle_file_upload_and_processing()

def handle_file_upload_and_processing():
    """Gérer l'upload et le traitement des fichiers"""
    st.subheader("📤 Upload des fichiers PDF")
    uploaded_files = st.file_uploader(
        "Choisissez vos fichiers PDF", 
        type="pdf",
        accept_multiple_files=True,
        help="Vous pouvez sélectionner plusieurs fichiers PDF à traiter"
    )
    
    if uploaded_files:
        show_uploaded_files_info(uploaded_files)
        
        if st.button("🚀 Lancer l'extraction de tous les PDF", type="primary"):
            process_uploaded_files(uploaded_files)
    else:
        st.info("👆 Veuillez uploader un ou plusieurs fichiers PDF pour commencer")

def show_uploaded_files_info(uploaded_files):
    """Afficher les informations des fichiers uploadés"""
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

def process_uploaded_files(uploaded_files):
    """Traiter les fichiers uploadés"""
    with st.spinner(f"🔍 Traitement de {len(uploaded_files)} fichier(s) PDF en cours..."):
        
        all_logs = []
        all_results = {}
        total_success = 0
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                
                for i, uploaded_file in enumerate(uploaded_files):
                    progress = (i) / len(uploaded_files)
                    progress_bar.progress(progress)
                    status_text.text(f"Traitement de {uploaded_file.name} ({i+1}/{len(uploaded_files)})")
                    
                    # Créer fichier temporaire
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                        tmp_file.write(uploaded_file.getvalue())
                        temp_pdf_path = tmp_file.name
                    
                    try:
                        # Traiter le PDF
                        def run_single_extraction():
                            return process_single_pdf(temp_pdf_path, uploaded_file.name, temp_dir)
                        
                        result, output = capture_prints(run_single_extraction)
                        all_logs.append(f"\n{'='*60}")
                        all_logs.append(f"PDF: {uploaded_file.name}")
                        all_logs.append(f"{'='*60}")
                        all_logs.append(output)
                        
                        all_results[uploaded_file.name] = result
                        
                        if result['csv_data']:
                            total_success += 1
                            
                    except Exception as e:
                        all_logs.append(f"\n❌ ERREUR pour {uploaded_file.name}: {e}")
                        all_results[uploaded_file.name] = create_empty_result(uploaded_file.name)
                    finally:
                        if os.path.exists(temp_pdf_path):
                            os.unlink(temp_pdf_path)
                
                # Créer le CSV global consolidé
                def run_global_csv_creation():
                    return create_global_csv(all_results)
                
                global_csv_data, global_output = capture_prints(run_global_csv_creation)
                all_logs.append(f"\n{'='*60}")
                all_logs.append("CONSOLIDATION GLOBALE")
                all_logs.append(f"{'='*60}")
                all_logs.append(global_output)
                
                # Finaliser
                finalize_processing(
                    all_results, global_csv_data, all_logs, 
                    uploaded_files, total_success, progress_bar, status_text
                )
        
        except Exception as e:
            st.error(f"❌ Erreur générale lors du traitement: {e}")
            st.exception(e)

def create_empty_result(filename):
    """Créer un résultat vide en cas d'erreur"""
    return {
        'pdf_filename': filename,
        'csv_filepath': None,
        'processing_results': {},
        'success_count': 0,
        'csv_data': None,
        'coverage_info': {},
        'dictionnaire_plages': {},
        'merged_dataframe': None
    }

def finalize_processing(all_results, global_csv_data, all_logs, uploaded_files, total_success, progress_bar, status_text):
    """Finaliser le traitement"""
    progress_bar.progress(1.0)
    status_text.text("✅ Traitement terminé!")
    
    # Sauvegarder dans session state
    st.session_state.all_results = all_results
    st.session_state.global_csv_data = global_csv_data
    st.session_state.output_log = "\n".join(all_logs)
    st.session_state.total_processed = len(uploaded_files)
    st.session_state.total_success = total_success
    st.session_state.extraction_done = True
    
    # Message de succès
    success_msg = f"🎉 Traitement terminé ! {total_success}/{len(uploaded_files)} PDF traités avec succès"
    if global_csv_data:
        success_msg += f"\n🌐 CSV global consolidé créé avec succès !"
    st.success(success_msg)
    
    # Recharger pour afficher les résultats
    st.rerun()

if __name__ == "__main__":
    main()
