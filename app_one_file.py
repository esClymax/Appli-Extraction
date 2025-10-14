"""
Point d'entrée principal de l'application Extracteur Multi-PDF vers CSV Global
"""

import streamlit as st
import pandas as pd
import pdfplumber
import re, os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import PyPDF2
import io
import sys
import tempfile
from datetime import datetime
import zipfile

# Configuration de la page Streamlit
st.set_page_config(
    page_title="Extracteur Multi-PDF vers CSV Global",
    page_icon="📊",
    layout="wide"
)

# Initialiser le session state
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

# Dictionnaire des bordereaux
dico_bordereau = {
    "Bordereau A1 n": "Admissions au stage statutaire",
    "Bordereau I2 n": "Suivis au stage statutaire",
    "Bordereau A3 n": "Titularisations",
    "Bordereau A4 n": "Reclassements",
    "Bordereau A5 n": "Publications - examen des candidatures",
    "Bordereau A50 n": "Nominations suite aux publications de postes",
    "Bordereau A6 n": "Mutations individuelles",
    "Bordereau A6 bis n": "Mutations collectives",
    "Bordereau A7 n": "Avancement",
    "Bordereau A7 bis n": "Avancement AIC",
    "Bordereau A7 ter n": "Reconnaissances individuelles au choix",
    "Bordereau I8 n": "Services civils",
    "Bordereau A9 n": "Requêtes individuelles"
}

def creer_dictionnaire_plages_mots_cles(chemin_pdf, mes_mots_cles, ignorer_casse=True):
    """Fonction pour créer le dictionnaire des plages de pages par mots-clés"""
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

@dataclass
class DictionaryExtractionConfig:
    pdf_path: str
    page_ranges_dict: Dict[str, List[str]]
    output_directory: str = "extracted_categories"
    extraction_methods: List[str] = None
    cleaning_rules: Dict[str, Any] = None
    column_mapping: Dict[str, str] = None
    filters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.extraction_methods is None:
            self.extraction_methods = ["pdfplumber"]
        if self.cleaning_rules is None:
            self.cleaning_rules = {}
        if self.column_mapping is None:
            self.column_mapping = {}
        if self.filters is None:
            self.filters = {}

class FileNameSanitizer:
    @staticmethod
    def sanitize_filename(name: str) -> str:
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
        sanitized = re.sub(r'\s+', '_', sanitized)
        sanitized = sanitized.strip('._-')
        sanitized = sanitized[:50] if len(sanitized) > 50 else sanitized
        return sanitized

class PageRangeParser:
    @staticmethod
    def parse_range(page_range: str) -> List[int]:
        if '-' in page_range:
            start, end = page_range.split('-')
            return list(range(int(start), int(end) + 1))
        else:
            return [int(page_range)]
    
    @staticmethod
    def parse_multiple_ranges(page_ranges: List[str]) -> List[int]:
        all_pages = []
        for range_str in page_ranges:
            all_pages.extend(PageRangeParser.parse_range(range_str))
        return sorted(list(set(all_pages)))

class PDFPlumberExtractor:
    def extract_ranges(self, pdf_path: str, page_ranges: List[str], category_name: str) -> List[pd.DataFrame]:
        try:
            print(f"    📄 PDFPlumber: extraction plages {page_ranges}")
            
            all_pages = PageRangeParser.parse_multiple_ranges(page_ranges)
            tables = []
            
            with pdfplumber.open(pdf_path) as pdf:
                for page_num in all_pages:
                    if page_num <= len(pdf.pages):
                        page = pdf.pages[page_num - 1]
                        page_tables = page.extract_tables()
                        
                        for table in page_tables:
                            if table and len(table) > 1:
                                cleaned_table = []
                                for row in table:
                                    cleaned_row = [cell if cell is not None else "" for cell in row]
                                    cleaned_table.append(cleaned_row)
                                
                                if cleaned_table:
                                    df = pd.DataFrame(cleaned_table[1:], columns=cleaned_table[0])
                                    
                                    if category_name == "Bordereau A5 n":
                                        with open(pdf_path, 'rb') as fichier:
                                            lecteur = PyPDF2.PdfReader(fichier)
                                            page = lecteur.pages[page_num - 1]
                                            texte_page = page.extract_text()

                                        lignes = texte_page.split('\n')

                                        UM_code = UM_char = DUM_code = DUM_char = SDUM_code = SDUM_char = None
                                        FSDUM_code = FSDUM_char = None
                                        Emploi = Lieu_de_travail = Publie_sous_le = Nombre_demploi = None
                                        Date_de_forclusion = Motif = Position = GF_de_publication = None
                                        CERNE = Reference_My_HR = None

                                        # Patterns regex
                                        pattern_ligne1 = r"Emploi :(.*?)Lieu de travail(.*?)Publié sous le n°(.+)"
                                        pattern_ligne3 = r"Motif(.*?)Position(.*?)GF de publication(.+)"
                                        pattern_ligne4 = r"CERNE\s*:\s*(.*?)\s+Référence MyHR\s+(.+)"
                                        
                                        for ligne in lignes:
                                            ligne = ligne.strip()
                                            
                                            if ligne.startswith('UM :'):
                                                parts = ligne.split(' ')
                                                if len(parts) >= 3:
                                                    UM_code = parts[2].strip()
                                                    UM_char = ' '.join(parts[3:]).strip() if len(parts) > 3 else None

                                            elif ligne.startswith('DUM :'):
                                                parts = ligne.split(' ')
                                                if len(parts) >= 3:
                                                    DUM_code = parts[2].strip()
                                                    DUM_char = ' '.join(parts[3:]).strip() if len(parts) > 3 else None

                                            elif ligne.startswith('SDUM :'):
                                                parts = ligne.split(' ')
                                                if len(parts) >= 3:
                                                    SDUM_code = parts[2].strip()
                                                    SDUM_char = ' '.join(parts[3:]).strip() if len(parts) > 3 else None

                                            elif ligne.startswith('FSDUM :'):
                                                parts = ligne.split(' ')
                                                if len(parts) >= 3:
                                                    FSDUM_code = parts[2].strip()
                                                    FSDUM_char = ' '.join(parts[3:]).strip() if len(parts) > 3 else None

                                            elif ligne.startswith('Emploi :'):
                                                        match1 = re.search(pattern_ligne1, ligne)
                                                        if match1:
                                                            Emploi = match1.group(1).strip()
                                                            Lieu_de_travail = match1.group(2).strip()
                                                            Publie_sous_le = match1.group(3).strip()

                                            elif ligne.startswith("Nombre d'emploi(s) "):
                                                parts = ligne.split(' ')
                                                if len(parts) >= 3:
                                                    Nombre_demploi = parts[2].strip()
                                                    if len(parts) > 3:
                                                        remaining = ' '.join(parts[3:])
                                                        if "Date de forclusion" in remaining:
                                                            location_part = remaining.split("Date de forclusion")[0].strip()
                                                            date_part = remaining.split("Date de forclusion")[1].strip()
                                                            if Lieu_de_travail and location_part:
                                                                Lieu_de_travail = Lieu_de_travail + ' ' + location_part
                                                            elif location_part:
                                                                Lieu_de_travail = location_part
                                                            Date_de_forclusion = date_part

                                            elif ligne.startswith('Motif '):
                                                match3 = re.search(pattern_ligne3, ligne)
                                                if match3:
                                                    Motif = match3.group(1).strip()
                                                    Position = match3.group(2).strip()
                                                    GF_de_publication = match3.group(3).strip()

                                            elif ligne.startswith('CERNE :'):
                                                # Nettoyage des espaces insécables et normalisation des espaces
                                                ligne_clean = ligne.replace('\xa0', ' ')
                                                ligne_clean = re.sub(r'\s+', ' ', ligne_clean)
                                                match4 = re.search(pattern_ligne4, ligne_clean)
                                                if match4:
                                                    CERNE = match4.group(1).strip()
                                                    Reference_My_HR = match4.group(2).strip()
                                        
                                        colonnes_postes = ['UM_code', 'UM_char', 'DUM_code', 'DUM_char', 'SDUM_code', 'SDUM_char', 'FSDUM_code', 'FSDUM_char', 
                                                           'Emploi_candidature', 'Lieu_de_travail', 'Publié_sous_le', 'Nombre_demploi', 'Date_de_forclusion', 
                                                            'Motif', 'Position', 'GF_de_publication', 'CERNE', 'Reference_My_HR']
                                        valeurs_postes = [[UM_code, UM_char, DUM_code, DUM_char, SDUM_code, SDUM_char, 
                                                        FSDUM_code, FSDUM_char, Emploi, Lieu_de_travail, Publie_sous_le, 
                                                        Nombre_demploi, Date_de_forclusion, Motif, Position, 
                                                        GF_de_publication, CERNE, Reference_My_HR]]*df.shape[0]
                                    
                                        df_postes = pd.DataFrame(data=valeurs_postes, columns=colonnes_postes)
                                        df_concat = pd.concat([df, df_postes], axis=1)
                                    
                                    tables.append(df_concat if category_name == "Bordereau A5 n" else df)
            
            print(f"      ✅ {len(tables)} tableaux extraits avec PDFPlumber")
            return tables
            
        except Exception as e:
            print(f"      ❌ Erreur PDFPlumber: {e}")
            return []

class DataCleaner:
    def __init__(self, cleaning_rules: Dict[str, Any]):
        self.rules = cleaning_rules
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
            
        df_clean = df.copy()
        
        if self.rules.get('remove_empty_rows', True):
            df_clean = df_clean.dropna(how='all')
        
        if self.rules.get('remove_empty_columns', True):
            df_clean = df_clean.dropna(axis=1, how='all')
        
        if self.rules.get('strip_whitespace', True):
            df_clean = df_clean.map(
                lambda x: x.strip() if isinstance(x, str) else x
            )
        
        regex_rules = self.rules.get('regex_patterns', {})
        for column, patterns in regex_rules.items():
            if column in df_clean.columns:
                for pattern, replacement in patterns.items():
                    df_clean[column] = df_clean[column].astype(str).str.replace(
                        pattern, replacement, regex=True
                    )
        
        return df_clean

class CategoryProcessor:
    def __init__(self, config: DictionaryExtractionConfig):
        self.config = config
        self.pdfplumber_extractor = PDFPlumberExtractor()
        self.cleaner = DataCleaner(config.cleaning_rules)
    
    def process_category(self, category_name: str, page_ranges: List[str]) -> Optional[pd.DataFrame]:
        all_tables = []
        
        for method in self.config.extraction_methods:
            tables = self.pdfplumber_extractor.extract_ranges(self.config.pdf_path, page_ranges, category_name)
            all_tables.extend(tables)
        
        if not all_tables:
            return None
        
        cleaned_tables = [self.cleaner.clean_dataframe(table) for table in all_tables]
        cleaned_tables = [table for table in cleaned_tables if table is not None and not table.empty]
        
        if not cleaned_tables:
            return None
        
        final_df = self._combine_tables(cleaned_tables)
        final_df = self._apply_transformations(final_df)
        
        return final_df
    
    def _combine_tables(self, tables: List[pd.DataFrame]) -> pd.DataFrame:
        if len(tables) == 1:
            return tables[0].reset_index(drop=True)
        
        try:
            # Réinitialiser les index avant concaténation
            clean_tables = []
            for table in tables:
                if table is not None and not table.empty:
                    clean_tables.append(table.reset_index(drop=True))
            
            if clean_tables:
                return pd.concat(clean_tables, ignore_index=True, sort=False)
            else:
                return pd.DataFrame()
        except Exception as e:
            print(f"Erreur combinaison tables: {e}")
            return max(tables, key=len).reset_index(drop=True) if tables else pd.DataFrame()
    
    def _apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df
            
        if self.config.column_mapping:
            df = df.rename(columns=self.config.column_mapping)
        
        for column, filter_config in self.config.filters.items():
            if column not in df.columns:
                continue
                
            filter_type = filter_config.get('type', 'contains')
            filter_value = filter_config.get('value', '')
            
            if filter_type == 'contains':
                df = df[df[column].astype(str).str.contains(filter_value, na=False)]
            elif filter_type == 'equals':
                df = df[df[column] == filter_value]
            elif filter_type == 'not_empty':
                df = df[df[column].notna()]
        
        return df

class DictionaryCSVProcessor:
    def __init__(self, config: DictionaryExtractionConfig):
        self.config = config
        self.category_processor = CategoryProcessor(config)
        os.makedirs(config.output_directory, exist_ok=True)
    
    def process_all_categories(self, pdf_filename: str) -> tuple:
        print(f"🟢 Début du traitement de {len(self.config.page_ranges_dict)} catégories")
        print(f"📁 Répertoire de sortie: {self.config.output_directory}")
        
        # Utiliser le nom du PDF pour le fichier CSV
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
                # Création du masque permettant de détecter les colonnes vides ou nommées "Unnamed"
                mask_unnamed = [
                    (c is None) or (isinstance(c, str) and (c.strip() == "" or c.lower().startswith("unnamed:")))
                    for c in df.columns
                ]
                
                # Renommer les colonnes vides ou "Unnamed"
                if True in mask_unnamed:
                    new_cols = []
                    compte = 0
                    consec = 0
                    left_name = ""
                    for i, c in enumerate(df.columns):
                        if mask_unnamed[i]:
                            consec += 1
                            compte += 1
                            if consec == 1:
                                left_name = new_cols[i-1] if i > 0 else "col0"
                                left_first_val = df.iloc[0, i-1] 
                                new_cols[i-1] = f"{left_name}_{str(left_first_val).strip()}"
                            first_val = df.iloc[0, i] 
                            new_cols.append(f"{left_name}_{str(first_val).strip()}")
                        else:
                            consec = 0
                            new_cols.append(str(c))
                
                    if compte > 0:
                        df = df.iloc[1:].reset_index(drop=True)
            
                    # Appliquer les nouveaux noms
                    df.columns = new_cols
                
                # Nettoyer les noms de colonnes
                df = self._clean_column_names(df)
                
                # Spécifique à "Bordereau A5 n"
                if category_name == "Bordereau A5 n":
                    # Correspondance exacte (en ignorant la casse)
                    if len(df.columns) > 5:
                        try:
                            mask = df.iloc[:, 5].str.lower().str.strip() == 'aucune candidature'
                            # Transférer vers la 1ère colonne
                            df.loc[mask, df.columns[0]] = 'aucune candidature'
                            # Vider la 5ème colonne pour ces lignes
                            df.loc[mask, df.columns[5]] = ''
                        except:
                            pass

                # Supprimer les lignes vides ou non pertinentes
                mask = (df.iloc[:, 0].astype(str).str.strip() != '')
                df = df[mask].reset_index(drop=True)
                
                # NOUVEAUTÉ : Ajouter la colonne "Document" en première position
                document_name = os.path.splitext(pdf_filename)[0]  # Nom du PDF sans extension
                df.insert(0, 'Document', document_name)
                
                # Ajouter la colonne catégorie en deuxième position
                category_label = dico_bordereau[category_name]
                df.insert(1, 'Catégorie', category_label)
                
                # Identifier et standardiser la colonne "Nom & Prénom"
                df = self._standardize_name_column(df)
                
                all_dataframes.append(df)
                
                processing_results[category_name] = {
                    'success': True,
                    'category_label': category_label,
                    'rows': len(df),
                    'cols': len(df.columns)
                }
                success_count += 1
                
                print(f"    ✅ Préparé: {category_label} ({df.shape[0]} lignes, {df.shape[1]} colonnes)")
            else:
                print(f"    ❌ Échec pour la catégorie '{category_name}'")
                processing_results[category_name] = {'success': False, 'error': 'Aucun tableau trouvé'}
        
        csv_data = None
        merged_df = None
        if all_dataframes:
            try:
                # Concaténer tous les DataFrames
                merged_df = self._concatenate_all_dataframes(all_dataframes)
                
                if merged_df is not None and not merged_df.empty:
                    # Créer le CSV
                    csv_buffer = io.StringIO()
                    merged_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
                    csv_data = csv_buffer.getvalue().encode('utf-8-sig')
                    
                    # Sauvegarder le fichier CSV
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
    
    def _clean_column_names(self, df):
        """Nettoie les noms de colonnes en supprimant les caractères de nouvelle ligne et autres caractères indésirables"""
        
        # Afficher les noms de colonnes suspects pour diagnostic
        for i, col in enumerate(df.columns):
            col_str = str(col)
            if '\n' in col_str or '\r' in col_str or '\t' in col_str:
                print(f"    🔧 Colonne {i} contient des caractères spéciaux: {repr(col_str)}")
        
        # Nettoyer les noms de colonnes
        cleaned_columns = []
        for col in df.columns:
            col_str = str(col)
            # Remplacer les retours à la ligne, tabulations par des espaces
            cleaned_col = col_str.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            # Nettoyer les espaces multiples
            cleaned_col = re.sub(r'\s+', ' ', cleaned_col)
            # Supprimer les espaces en début et fin
            cleaned_col = cleaned_col.strip()
            cleaned_columns.append(cleaned_col)
        
        # Appliquer les nouveaux noms
        df.columns = cleaned_columns
        
        return df
    
    def _standardize_name_column(self, df):
        """Standardise le nom de la colonne contenant les noms et prénoms"""
        # Chercher une colonne qui pourrait contenir les noms
        name_patterns = [
            r'nom.*pr[eé]nom',
            r'pr[eé]nom.*nom', 
            r'^nom$',
            r'nom',
            r'pr[eé]nom',
            r'identit[eé]',
            r'personne'
        ]
        
        for col in df.columns:
            col_lower = str(col).lower()
            for pattern in name_patterns:
                if re.search(pattern, col_lower):
                    # Renommer cette colonne
                    df = df.rename(columns={col: 'Nom & Prénom'})
                    return df
        
        # Si aucune colonne trouvée, prendre la première après "Document" et "Catégorie"
        if len(df.columns) > 2:
            df = df.rename(columns={df.columns[2]: 'Nom & Prénom'})
        
        return df
    
    def _concatenate_all_dataframes(self, dataframes_list):
        """
        Concatène tous les DataFrames sans fusion sur Nom & Prénom
        Chaque ligne reste distincte, même si le nom est identique
        """
        if not dataframes_list:
            return pd.DataFrame()
            
        if len(dataframes_list) == 1:
            return dataframes_list[0]
        
        try:
            print(f"🔗 Concaténation de {len(dataframes_list)} DataFrames...")
            
            # Nettoyer tous les DataFrames d'abord
            clean_dataframes = []
            for i, df in enumerate(dataframes_list):
                if df is not None and not df.empty:
                    # Forcer la réinitialisation de l'index
                    clean_df = df.copy().reset_index(drop=True)
                    
                    # Vérifier les colonnes dupliquées
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
            
            # Concaténer tous les DataFrames les uns sous les autres
            merged_df = pd.concat(clean_dataframes, ignore_index=True, sort=False)
            
            # Réorganiser les colonnes : Document, Catégorie, Nom & Prénom en premier
            cols_to_front = []
            if 'Document' in merged_df.columns:
                cols_to_front.append('Document')
            if 'Catégorie' in merged_df.columns:
                cols_to_front.append('Catégorie')
            if 'Nom & Prénom' in merged_df.columns:
                cols_to_front.append('Nom & Prénom')
            
            # Ajouter toutes les autres colonnes
            remaining_cols = [col for col in merged_df.columns if col not in cols_to_front]
            final_columns_order = cols_to_front + remaining_cols
            
            # Réorganiser le DataFrame selon cet ordre
            merged_df = merged_df[final_columns_order]
            
            # Remplir les valeurs NaN avec des chaînes vides pour un CSV plus propre
            merged_df = merged_df.fillna('')
            
            print(f"   ✅ Concaténation réussie: {len(merged_df)} lignes totales")
            
            return merged_df
            
        except Exception as e:
            print(f"❌ Erreur concaténation: {e}")
            # En cas d'erreur, retourner le plus grand DataFrame
            if dataframes_list:
                largest = max(dataframes_list, key=lambda x: len(x) if x is not None else 0)
                return largest.reset_index(drop=True) if largest is not None else pd.DataFrame()
            return pd.DataFrame()

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
        "Bordereau A4 n", "Bordereau A5 n", "Bordereau A50 n", 
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
    
    # Traitement CSV
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
        # Nettoyer tous les DataFrames avant concaténation
        clean_global_dataframes = []
        for i, df in enumerate(all_global_dataframes):
            if df is not None and not df.empty:
                clean_df = df.copy().reset_index(drop=True)
                clean_global_dataframes.append(clean_df)
        
        if not clean_global_dataframes:
            return None
        
        # Concaténer tous les DataFrames
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
        
        # NOUVELLE SECTION : CSV Global consolidé
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
                    # Téléchargement du CSV global
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
                
                # Aperçu du CSV global
                st.write("**👀 Aperçu du CSV Global**")
                st.dataframe(global_preview_df.head(15), use_container_width=True)
                
                # Statistiques du CSV global
                if len(global_preview_df) > 0:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Répartition par document
                        if 'Document' in global_preview_df.columns:
                            doc_counts = global_preview_df['Document'].value_counts()
                            st.write("**📄 Répartition par Document :**")
                            st.bar_chart(doc_counts)
                    
                    with col2:
                        # Répartition par catégorie
                        if 'Catégorie' in global_preview_df.columns:
                            category_counts = global_preview_df['Catégorie'].value_counts()
                            st.write("**📈 Répartition par Catégorie :**")
                            st.bar_chart(category_counts)
                
                # Tableau croisé dynamique
                if 'Document' in global_preview_df.columns and 'Catégorie' in global_preview_df.columns:
                    st.write("**📊 Tableau croisé : Documents vs Catégories**")
                    cross_tab = pd.crosstab(global_preview_df['Document'], global_preview_df['Catégorie'])
                    st.dataframe(cross_tab, use_container_width=True)
                    
            except Exception as e:
                st.error(f"Erreur lors de l'affichage du CSV global: {e}")
        else:
            st.warning("❌ Aucun CSV global n'a pu être créé")
        
        # Résultats par PDF
        st.markdown("---")
        st.subheader("📂 Résultats individuels par PDF")
        
        for pdf_name, result in st.session_state.all_results.items():
            with st.expander(f"📄 {pdf_name}", expanded=False):
                
                # Statut général
                if result['csv_data']:
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
                            st.dataframe(recap_df, use_container_width=True, height=200)
                
                # Téléchargement CSV individuel
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
                    
                    # Aperçu des données CSV individuelles
                    with st.expander(f"👀 Aperçu des données de {pdf_name}"):
                        try:
                            csv_content = result['csv_data'].decode('utf-8-sig')
                            preview_df = pd.read_csv(io.StringIO(csv_content))
                            st.info(f"📊 {len(preview_df)} lignes, {len(preview_df.columns)} colonnes")
                            st.dataframe(preview_df.head(5), use_container_width=True)
                            
                            # Afficher les doublons de noms pour information
                            if 'Nom & Prénom' in preview_df.columns:
                                duplicate_names = preview_df[preview_df.duplicated(subset=['Nom & Prénom'], keep=False)]
                                if not duplicate_names.empty:
                                    st.info(f"🔄 {len(duplicate_names)} lignes avec des noms en double (personnes dans plusieurs catégories)")
                        except Exception as e:
                            st.error(f"Erreur lors de la lecture du CSV: {e}")
        
        # Téléchargement ZIP de tous les CSV individuels
        st.markdown("---")
        st.subheader("📦 Téléchargement groupé des CSV individuels")
        
        successful_csvs = {name: result for name, result in st.session_state.all_results.items() 
                          if result['csv_data'] is not None}
        
        if len(successful_csvs) > 1:
            # Créer le fichier ZIP avec tous les CSV
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

# Interface principale
def main():
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
                                
                                if result['csv_data']:
                                    total_success += 1
                                    
                            except Exception as e:
                                all_logs.append(f"\n❌ ERREUR pour {uploaded_file.name}: {e}")
                                all_results[uploaded_file.name] = {
                                    'pdf_filename': uploaded_file.name,
                                    'csv_filepath': None,
                                    'processing_results': {},
                                    'success_count': 0,
                                    'csv_data': None,
                                    'coverage_info': {},
                                    'dictionnaire_plages': {},
                                    'merged_dataframe': None
                                }
                            finally:
                                # Nettoyer le fichier temporaire
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
                        
                        # Finaliser la progression
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
                
                except Exception as e:
                    st.error(f"❌ Erreur générale lors du traitement: {e}")
                    st.exception(e)
    
    else:
        st.info("👆 Veuillez uploader un ou plusieurs fichiers PDF pour commencer")

if __name__ == "__main__":
    main()
