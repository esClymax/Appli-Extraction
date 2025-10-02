import PyPDF2
import io
import sys
from config import dico_bordereau
from fonctions_traitement_dataframes import DictionaryExtractionConfig, PageRangeParser
from fonctions_traitement_excel import DictionaryExcelProcessor

def creer_dictionnaire_plages_mots_cles(chemin_pdf, mes_mots_cles, ignorer_casse=True):
    """Créer un dictionnaire des plages de pages pour chaque mot-clé"""
    def regrouper_pages_consecutives(pages_list):
        if not pages_list:
            return []
        
        pages_list = sorted(set(pages_list))
        plages = []
        debut = pages_list[0]
        fin = pages_list[0]
        
        for i in range(1, len(pages_list)):
            if pages_list[i] == fin + 1:
                fin = pages_list[i]
            else:
                if debut == fin:
                    plages.append(f"{debut}-{debut}")
                else:
                    plages.append(f"{debut}-{fin}")
                debut = fin = pages_list[i]
        
        if debut == fin:
            plages.append(f"{debut}-{debut}")
        else:
            plages.append(f"{debut}-{fin}")
        
        return plages
    
    dictionnaire_plages = {mot_cle: [] for mot_cle in mes_mots_cles}
    
    try:
        with open(chemin_pdf, 'rb') as fichier:
            lecteur_pdf = PyPDF2.PdfReader(fichier)
            nb_pages_total = len(lecteur_pdf.pages)
            
            print(f"📄 Analyse de {nb_pages_total} pages pour {len(mes_mots_cles)} mots-clés...")
            
            pages_par_mot_cle = {mot_cle: [] for mot_cle in mes_mots_cles}
            
            for numero_page in range(nb_pages_total):
                page = lecteur_pdf.pages[numero_page]
                texte_page = page.extract_text()
                
                texte_recherche = texte_page.lower() if ignorer_casse else texte_page
                
                for mot_cle in mes_mots_cles:
                    mot_cle_recherche = mot_cle.lower() if ignorer_casse else mot_cle
                    
                    if mot_cle_recherche in texte_recherche:
                        pages_par_mot_cle[mot_cle].append(numero_page + 1)
                    elif dico_bordereau[mot_cle].lower() in texte_recherche:
                        pages_par_mot_cle[mot_cle].append(numero_page + 1)
                    
            for mot_cle in mes_mots_cles:
                if pages_par_mot_cle[mot_cle]:
                    plages = regrouper_pages_consecutives(pages_par_mot_cle[mot_cle])
                    dictionnaire_plages[mot_cle] = plages
                    
            return dictionnaire_plages
            
    except Exception as e:
        print(f"❌ Erreur lors de l'analyse du PDF : {e}")
        return {}

def calculate_coverage_info(pdf_path, dictionnaire_plages):
    """Calculer les informations de recouvrement du document"""
    try:
        with open(pdf_path, 'rb') as fichier:
            lecteur_pdf = PyPDF2.PdfReader(fichier)
            total_pages = len(lecteur_pdf.pages)
        
        pages_traitees = set()
        for category, page_ranges in dictionnaire_plages.items():
            if page_ranges:
                for range_str in page_ranges:
                    pages_range = PageRangeParser.parse_range(range_str)
                    pages_traitees.update(pages_range)
        
        toutes_pages = set(range(1, total_pages + 1))
        pages_non_traitees = toutes_pages - pages_traitees
        
        pourcentage_couverture = (len(pages_traitees) / total_pages) * 100 if total_pages > 0 else 0
        
        coverage_info = {
            'total_pages': total_pages,
            'pages_traitees': sorted(list(pages_traitees)),
            'pages_non_traitees': sorted(list(pages_non_traitees)),
            'nb_pages_traitees': len(pages_traitees),
            'nb_pages_non_traitees': len(pages_non_traitees),
            'pourcentage_couverture': round(pourcentage_couverture, 1)
        }
        
        return coverage_info
        
    except Exception as e:
        print(f"❌ Erreur lors du calcul de couverture : {e}")
        return {
            'total_pages': 0,
            'pages_traitees': [],
            'pages_non_traitees': [],
            'nb_pages_traitees': 0,
            'nb_pages_non_traitees': 0,
            'pourcentage_couverture': 0
        }

def capture_prints(func, *args, **kwargs):
    """Capture les prints d'une fonction"""
    old_stdout = sys.stdout
    captured_output = io.StringIO()
    
    try:
        sys.stdout = captured_output
        result = func(*args, **kwargs)
        output = captured_output.getvalue()
        return result, output
    finally:
        sys.stdout = old_stdout

def process_single_pdf(pdf_path, pdf_filename, temp_dir):
    """Traiter un seul PDF"""
    mes_mots_cles = [
        "Bordereau A1 n", "Bordereau I2 n", "Bordereau A3 n", 
        "Bordereau A4 n", "Bordereau A5 n", "Bordereau A10 n", 
        "Bordereau A6 n", "Bordereau A6 bis n", "Bordereau A7 n", 
        "Bordereau A7 bis n", "Bordereau A7 ter n", "Bordereau I8 n", 
        "Bordereau A9 n"
    ]
    
    print(f"\n{'='*60}")
    print(f"🔍 TRAITEMENT: {pdf_filename}")
    print(f"{'='*60}")
    
    # Analyser le PDF
    dictionnaire_plages = creer_dictionnaire_plages_mots_cles(
        pdf_path, mes_mots_cles, ignorer_casse=True
    )
    
    # Calculer la couverture
    coverage_info = calculate_coverage_info(pdf_path, dictionnaire_plages)
    
    # Traitement Excel
    config = DictionaryExtractionConfig(
        pdf_path=pdf_path,
        page_ranges_dict=dictionnaire_plages,
        output_directory=temp_dir,
        cleaning_rules={
            'remove_empty_rows': True,
            'remove_empty_columns': True,
            'strip_whitespace': True,
        }
    )
    
    processor = DictionaryExcelProcessor(config)
    excel_filepath, processing_results, success_count, excel_data = processor.process_all_categories(pdf_filename)
    
    return {
        'pdf_filename': pdf_filename,
        'excel_filepath': excel_filepath,
        'processing_results': processing_results,
        'success_count': success_count,
        'excel_data': excel_data,
        'coverage_info': coverage_info,
        'dictionnaire_plages': dictionnaire_plages
    }
