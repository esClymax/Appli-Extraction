import streamlit as st
import pandas as pd
import io
import tempfile
import os
import zipfile
from datetime import datetime

from traitements_fichiers import process_single_pdf, capture_prints
from fonctions_traitement_excel import FileNameSanitizer
from config import dico_bordereau

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Extracteur Multi-PDF vers Excel",
    page_icon="📊",
    layout="wide"
)

# Initialiser le session state
if 'extraction_done' not in st.session_state:
    st.session_state.extraction_done = False
if 'all_results' not in st.session_state:
    st.session_state.all_results = {}
if 'output_log' not in st.session_state:
    st.session_state.output_log = ""
if 'total_processed' not in st.session_state:
    st.session_state.total_processed = 0
if 'total_success' not in st.session_state:
    st.session_state.total_success = 0

def reset_extraction():
    """Remettre à zéro l'extraction"""
    st.session_state.extraction_done = False
    st.session_state.all_results = {}
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
        
        # Résultats par PDF
        st.subheader("📂 Résultats par PDF")
        
        for pdf_name, result in st.session_state.all_results.items():
            with st.expander(f"📄 {pdf_name}", expanded=True):
                
                # Statut général
                if result['excel_data']:
                    st.success(f"✅ Traitement réussi - {result['success_count']} catégories extraites")
                else:
                    st.error("❌ Échec du traitement")
                
                col1, col2 = st.columns(2)
                
                # Informations de couverture
                with col1:
                    st.write("**📑 Couverture du document**")
                    coverage = result['coverage_info']
                    
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
                        def format_page_ranges(pages_list):
                            if not pages_list:
                                return "Aucune"
                            
                            ranges = []
                            start = pages_list[0]
                            end = pages_list[0]
                            
                            for i in range(1, len(pages_list)):
                                if pages_list[i] == end + 1:
                                    end = pages_list[i]
                                else:
                                    if start == end:
                                        ranges.append(str(start))
                                    else:
                                        ranges.append(f"{start}-{end}")
                                    start = end = pages_list[i]
                            
                            if start == end:
                                ranges.append(str(start))
                            else:
                                ranges.append(f"{start}-{end}")
                            
                            return ", ".join(ranges)
                        
                        st.warning(f"**Pages non traitées :** {format_page_ranges(coverage['pages_non_traitees'])}")
                    else:
                        st.success("🎉 **Toutes les pages ont été traitées !**")
                
                # Détails des catégories
                with col2:
                    st.write("**📋 Détail des catégories**")
                    
                    if result['processing_results']:
                        recap_data = []
                        for category, data_info in result['processing_results'].items():
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
                            st.dataframe(recap_df, use_container_width=True, height=300)
                
                # Téléchargement
                if result['excel_data']:
                    base_name = os.path.splitext(pdf_name)[0]
                    safe_base_name = FileNameSanitizer.sanitize_filename(base_name)
                    
                    st.download_button(
                        label=f"📊 Télécharger {pdf_name}.xlsx",
                        data=result['excel_data'],
                        file_name=f"{safe_base_name}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"download_{pdf_name}",
                        use_container_width=True
                    )
                    
                    # Aperçu des données
                    st.write("**👀 Aperçu des données**")
                    success_categories = [cat for cat, data_info in result['processing_results'].items() 
                                        if data_info.get('success')]
                    
                    if success_categories:
                        selected_category = st.selectbox(
                            f"Choisir une feuille à prévisualiser ({pdf_name}):",
                            success_categories,
                            format_func=lambda x: dico_bordereau[x],
                            key=f"preview_{pdf_name}"
                        )
                        
                        if selected_category:
                            try:
                                excel_buffer = io.BytesIO(result['excel_data'])
                                sheet_name = result['processing_results'][selected_category]['sheet_name']
                                preview_df = pd.read_excel(excel_buffer, sheet_name=sheet_name)
                                st.info(f"📊 {len(preview_df)} lignes, {len(preview_df.columns)} colonnes")
                                st.dataframe(preview_df.head(5), use_container_width=True)
                            except Exception as e:
                                st.error(f"Erreur lors de la lecture: {e}")
        
        # Téléchargement ZIP global de tous les Excel
        st.markdown("---")
        st.subheader("📦 Téléchargement groupé")
        
        # Compter les Excel réussis
        successful_excels = {name: result for name, result in st.session_state.all_results.items() 
                           if result['excel_data'] is not None}
        
        if len(successful_excels) > 1:
            # Créer le fichier ZIP avec tous les Excel
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for pdf_name, result in successful_excels.items():
                    if result['excel_data']:
                        base_name = os.path.splitext(pdf_name)[0]
                        safe_base_name = FileNameSanitizer.sanitize_filename(base_name)
                        excel_filename = f"{safe_base_name}.xlsx"
                        zip_file.writestr(excel_filename, result['excel_data'])
            
            zip_buffer.seek(0)
            
            col1, col2 = st.columns([2, 1])
            with col1:
                st.info(f"📊 {len(successful_excels)} fichiers Excel prêts à télécharger en une fois")
            with col2:
                st.metric("📦 Fichiers dans le ZIP", len(successful_excels))
            
            # Bouton de téléchargement du ZIP
            st.download_button(
                label=f"📦 Télécharger tous les fichiers Excel (ZIP)",
                data=zip_buffer.getvalue(),
                file_name=f"extraction_multi_pdf_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                mime="application/zip",
                key="download_all_excel_zip",
                use_container_width=True
            )
            
            # Détails du contenu du ZIP
            with st.expander("📋 Contenu du fichier ZIP"):
                zip_content = []
                for pdf_name in successful_excels.keys():
                    base_name = os.path.splitext(pdf_name)[0]
                    safe_base_name = FileNameSanitizer.sanitize_filename(base_name)
                    zip_content.append({
                        'PDF original': pdf_name,
                        'Fichier Excel dans le ZIP': f"{safe_base_name}.xlsx"
                    })
                
                zip_content_df = pd.DataFrame(zip_content)
                st.dataframe(zip_content_df, use_container_width=True, height=min(300, len(zip_content) * 35 + 50))
                
        elif len(successful_excels) == 1:
            st.info("📊 Un seul fichier Excel généré - utilisez le téléchargement individuel ci-dessus")
        else:
            st.warning("❌ Aucun fichier Excel généré avec succès")

def main():
    st.title("📊 Extracteur Multi-PDF vers Excel")
    st.markdown("---")
    
    # Si extraction terminée, afficher les résultats
    if st.session_state.extraction_done:
        show_results()
        return
    
    # Description
    st.markdown("""
    Cette application permet d'extraire des tableaux de **plusieurs fichiers PDF** et de les convertir chacun en un fichier Excel avec des feuilles organisées par catégories.
    
    **Comment l'utiliser :**
    1. 📤 Uploadez vos fichiers PDF (plusieurs acceptés)
    2. 🚀 Cliquez sur "Lancer l'extraction de tous les PDF"
    3. 📥 Téléchargez les fichiers Excel générés (un par PDF)
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
        # Afficher les informations des fichiers
        st.success(f"✅ {len(uploaded_files)} fichier(s) uploadé(s)")
        
        # Affichage personnalisé des fichiers
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
        
        # Bouton pour lancer l'extraction
        if st.button("🚀 Lancer l'extraction de tous les PDF", type="primary"):
            
            with st.spinner(f"🔍 Traitement de {len(uploaded_files)} fichier(s) PDF en cours..."):
                
                # Préparer les logs et résultats
                all_logs = []
                all_results = {}
                total_success = 0
                
                # Barre de progression
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    with tempfile.TemporaryDirectory() as temp_dir:
                        
                        for i, uploaded_file in enumerate(uploaded_files):
                            # Mise à jour de la progression
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
                                
                                # Stocker le résultat
                                all_results[uploaded_file.name] = result
                                
                                if result['excel_data']:
                                    total_success += 1
                                    
                            except Exception as e:
                                all_logs.append(f"\n❌ ERREUR pour {uploaded_file.name}: {e}")
                                all_results[uploaded_file.name] = {
                                    'pdf_filename': uploaded_file.name,
                                    'excel_filepath': None,
                                    'processing_results': {},
                                    'success_count': 0,
                                    'excel_data': None,
                                    'coverage_info': {},
                                    'dictionnaire_plages': {}
                                }
                            finally:
                                # Nettoyer le fichier temporaire
                                if os.path.exists(temp_pdf_path):
                                    os.unlink(temp_pdf_path)
                        
                        # Finaliser la progression
                        progress_bar.progress(1.0)
                        status_text.text("✅ Traitement terminé!")
                        
                        # Sauvegarder dans session state
                        st.session_state.all_results = all_results
                        st.session_state.output_log = "\n".join(all_logs)
                        st.session_state.total_processed = len(uploaded_files)
                        st.session_state.total_success = total_success
                        st.session_state.extraction_done = True
                        
                        # Message de succès
                        st.success(f"🎉 Traitement terminé ! {total_success}/{len(uploaded_files)} PDF traités avec succès")
                        
                        # Recharger pour afficher les résultats
                        st.rerun()
                
                except Exception as e:
                    st.error(f"❌ Erreur générale lors du traitement: {e}")
                    st.exception(e)
    
    else:
        st.info("👆 Veuillez uploader un ou plusieurs fichiers PDF pour commencer")

if __name__ == "__main__":
    main()
