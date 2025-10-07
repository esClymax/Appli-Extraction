"""
Analyse des fichiers PDF pour détecter les mots-clés et plages de pages
"""

import PyPDF2
from typing import Dict, List, Optional
from .utils import PageRangeParser
from config import DICO_BORDEREAU


class PDFAnalyzer:
    """Classe pour analyser les PDF et détecter les sections"""
    
    def __init__(self, ignore_case: bool = True):
        self.ignore_case = ignore_case
    
    def analyze_pdf(self, pdf_path: str, keywords: List[str]) -> Dict[str, List[str]]:
        """
        Analyse un PDF pour détecter les mots-clés et créer le dictionnaire des plages
        
        Args:
            pdf_path: Chemin vers le PDF
            keywords: Liste des mots-clés à rechercher
            
        Returns:
            Dictionnaire {mot_clé: [plages_pages]}
        """
        print(f"📄 Analyse du PDF: {pdf_path}")
        
        try:
            with open(pdf_path, 'rb') as fichier:
                lecteur_pdf = PyPDF2.PdfReader(fichier)
                total_pages = len(lecteur_pdf.pages)
                
                print(f"📄 Analyse de {total_pages} pages pour {len(keywords)} mots-clés...")
                
                # Détecter les pages contenant chaque mot-clé
                pages_by_keyword = self._detect_keywords_in_pages(
                    lecteur_pdf, keywords, total_pages
                )
                
                # Regrouper en plages consécutives
                page_ranges = self._create_page_ranges(pages_by_keyword)
                
                # Statistiques
                total_detected = sum(len(ranges) for ranges in page_ranges.values())
                print(f"✅ Analyse terminée: {total_detected} plages détectées")
                
                return page_ranges
                
        except Exception as e:
            print(f"❌ Erreur lors de l'analyse du PDF: {e}")
            return {keyword: [] for keyword in keywords}
    
    def _detect_keywords_in_pages(self, pdf_reader: PyPDF2.PdfReader, 
                                  keywords: List[str], total_pages: int) -> Dict[str, List[int]]:
        """
        Détecte les pages contenant chaque mot-clé
        
        Args:
            pdf_reader: Lecteur PDF
            keywords: Liste des mots-clés
            total_pages: Nombre total de pages
            
        Returns:
            Dictionnaire {mot_clé: [numéros_pages]}
        """
        pages_by_keyword = {keyword: [] for keyword in keywords}
        
        for page_num in range(total_pages):
            try:
                page = pdf_reader.pages[page_num]
                page_text = page.extract_text()
                
                if not page_text:
                    continue
                
                search_text = page_text.lower() if self.ignore_case else page_text
                
                # Vérifier chaque mot-clé
                for keyword in keywords:
                    if self._keyword_found_in_text(search_text, keyword):
                        pages_by_keyword[keyword].append(page_num + 1)  # Pages 1-indexed
                        
            except Exception as e:
                print(f"⚠️ Erreur page {page_num + 1}: {e}")
                continue
        
        return pages_by_keyword
    
    def _keyword_found_in_text(self, text: str, keyword: str) -> bool:
        """
        Vérifie si un mot-clé est présent dans le texte
        
        Args:
            text: Texte de la page
            keyword: Mot-clé à rechercher
            
        Returns:
            True si trouvé
        """
        search_keyword = keyword.lower() if self.ignore_case else keyword
        
        # Recherche directe du mot-clé
        if search_keyword in text:
            return True
        
        # Recherche du libellé correspondant depuis le dictionnaire
        if keyword in DICO_BORDEREAU:
            label = DICO_BORDEREAU[keyword]
            search_label = label.lower() if self.ignore_case else label
            if search_label in text:
                return True
        
        return False
    
    def _create_page_ranges(self, pages_by_keyword: Dict[str, List[int]]) -> Dict[str, List[str]]:
        """
        Crée les plages de pages consécutives
        
        Args:
            pages_by_keyword: Pages détectées par mot-clé
            
        Returns:
            Dictionnaire {mot_clé: [plages_string]}
        """
        page_ranges = {}
        
        for keyword, pages in pages_by_keyword.items():
            if pages:
                ranges = PageRangeParser.group_consecutive_pages(pages)
                page_ranges[keyword] = ranges
                print(f"  📋 {keyword}: pages {', '.join(ranges)}")
            else:
                page_ranges[keyword] = []
                print(f"  ❌ {keyword}: aucune page trouvée")
        
        return page_ranges


class CoverageAnalyzer:
    """Classe pour analyser la couverture d'un document"""
    
    @staticmethod
    def calculate_coverage(pdf_path: str, page_ranges_dict: Dict[str, List[str]]) -> Dict:
        """
        Calcule les informations de couverture du document
        
        Args:
            pdf_path: Chemin vers le PDF
            page_ranges_dict: Dictionnaire des plages par catégorie
            
        Returns:
            Informations de couverture
        """
        try:
            with open(pdf_path, 'rb') as fichier:
                lecteur_pdf = PyPDF2.PdfReader(fichier)
                total_pages = len(lecteur_pdf.pages)
            
            # Calculer les pages traitées
            processed_pages = set()
            for category, page_ranges in page_ranges_dict.items():
                if page_ranges:
                    for range_str in page_ranges:
                        pages_range = PageRangeParser.parse_range(range_str)
                        processed_pages.update(pages_range)
            
            # Calculer les pages non traitées
            all_pages = set(range(1, total_pages + 1))
            unprocessed_pages = all_pages - processed_pages
            
            # Calculer le pourcentage
            coverage_percentage = (len(processed_pages) / total_pages) * 100 if total_pages > 0 else 0
            
            return {
                'total_pages': total_pages,
                'pages_traitees': sorted(list(processed_pages)),
                'pages_non_traitees': sorted(list(unprocessed_pages)),
                'nb_pages_traitees': len(processed_pages),
                'nb_pages_non_traitees': len(unprocessed_pages),
                'pourcentage_couverture': round(coverage_percentage, 1)
            }
            
        except Exception as e:
            print(f"❌ Erreur lors du calcul de couverture: {e}")
            return {
                'total_pages': 0,
                'pages_traitees': [],
                'pages_non_traitees': [],
                'nb_pages_traitees': 0,
                'nb_pages_non_traitees': 0,
                'pourcentage_couverture': 0
            }
