"""
Classes et fonctions d'extraction PDF
"""

import pandas as pd
import pdfplumber
import PyPDF2
import re
from typing import List, Dict
from utils import PageRangeParser
from config import DICO_BORDEREAU


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
                    elif DICO_BORDEREAU[mot_cle].lower() in texte_recherche:
                        pages_par_mot_cle[mot_cle].append(numero_page + 1)
                    
            for mot_cle in mes_mots_cles:
                if pages_par_mot_cle[mot_cle]:
                    plages = regrouper_pages_consecutives(pages_par_mot_cle[mot_cle])
                    dictionnaire_plages[mot_cle] = plages
                    
            return dictionnaire_plages
            
    except Exception as e:
        print(f"❌ Erreur lors de l'analyse du PDF : {e}")
        return {}


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
                                        df_concat = self._extract_bordereau_a5_details(pdf_path, page_num, df)
                                        tables.append(df_concat)
                                    else:
                                        tables.append(df)
            
            print(f"      ✅ {len(tables)} tableaux extraits avec PDFPlumber")
            return tables
            
        except Exception as e:
            print(f"      ❌ Erreur PDFPlumber: {e}")
            return []
    
    def _extract_bordereau_a5_details(self, pdf_path: str, page_num: int, df: pd.DataFrame) -> pd.DataFrame:
        """Extraire les détails spécifiques au Bordereau A5"""
        with open(pdf_path, 'rb') as fichier:
            lecteur = PyPDF2.PdfReader(fichier)
            page = lecteur.pages[page_num - 1]
            texte_page = page.extract_text()

        lignes = texte_page.split('\n')

        # Initialisation des variables
        UM_code = UM_char = DUM_code = DUM_char = SDUM_code = SDUM_char = None
        FSDUM_code = FSDUM_char = None
        Emploi = Lieu_de_travail = Publie_sous_le = Nombre_demploi = None
        Date_de_forclusion = Motif = Position = GF_de_publication = None
        CERNE = Reference_My_HR = None

        # Patterns regex
        pattern_ligne1 = r"Emploi : (.*?) Lieu de travail (.*?) Publié sous le n° (.+)"
        pattern_ligne3 = r"Motif (.*?) Position (.*?) GF de publication (.+)"
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
                ligne_clean = ligne.replace('\xa0', ' ')
                ligne_clean = re.sub(r'\s+', ' ', ligne_clean)
                match4 = re.search(pattern_ligne4, ligne_clean)
                if match4:
                    CERNE = match4.group(1).strip()
                    Reference_My_HR = match4.group(2).strip()
        
        # Créer le DataFrame avec les données extraites
        colonnes_postes = ['UM_code', 'UM_char', 'DUM_code', 'DUM_char', 'SDUM_code', 'SDUM_char', 'FSDUM_code', 'FSDUM_char', 
                           'Emploi_candidature', 'Lieu_de_travail', 'Publié_sous_le', 'Nombre_demploi', 'Date_de_forclusion', 
                            'Motif', 'Position_candidature', 'GF_de_publication', 'CERNE', 'Reference_My_HR']
        valeurs_postes = [[UM_code, UM_char, DUM_code, DUM_char, SDUM_code, SDUM_char, 
                        FSDUM_code, FSDUM_char, Emploi, Lieu_de_travail, Publie_sous_le, 
                        Nombre_demploi, Date_de_forclusion, Motif, Position, 
                        GF_de_publication, CERNE, Reference_My_HR]] * df.shape[0]
    
        df_postes = pd.DataFrame(data=valeurs_postes, columns=colonnes_postes)
        df_concat = pd.concat([df, df_postes], axis=1)
        
        return df_concat
