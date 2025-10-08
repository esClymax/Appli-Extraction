"""
Utilitaires pour l'extraction PDF
"""

import re
import os
import io
import sys
from typing import List, Callable, Any, Tuple
import contextlib


class FileNameSanitizer:
    """Utilitaire pour nettoyer les noms de fichiers"""
    
    @staticmethod
    def sanitize_filename(name: str, max_length: int = 50) -> str:
        """
        Nettoie un nom de fichier en supprimant les caractères interdits
        
        Args:
            name: Nom à nettoyer
            max_length: Longueur maximale du nom
            
        Returns:
            Nom nettoyé
        """
        # Remplacer les caractères interdits
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
        # Remplacer les espaces multiples par un seul underscore
        sanitized = re.sub(r'\s+', '_', sanitized)
        # Supprimer les caractères en début et fin
        sanitized = sanitized.strip('._-')
        # Limiter la longueur
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length]
        return sanitized


class PageRangeParser:
    """Utilitaire pour parser les plages de pages"""
    
    @staticmethod
    def parse_range(page_range: str) -> List[int]:
        """
        Parse une plage de pages (ex: "1-5" -> [1,2,3,4,5])
        
        Args:
            page_range: Plage sous forme de string
            
        Returns:
            Liste des numéros de pages
        """
        try:
            if '-' in page_range:
                start, end = page_range.split('-')
                return list(range(int(start), int(end) + 1))
            else:
                return [int(page_range)]
        except ValueError as e:
            print(f"❌ Erreur lors du parsing de la plage '{page_range}': {e}")
            return []
    
    @staticmethod
    def parse_multiple_ranges(page_ranges: List[str]) -> List[int]:
        """
        Parse plusieurs plages de pages et retourne une liste consolidée
        
        Args:
            page_ranges: Liste de plages
            
        Returns:
            Liste triée et unique des numéros de pages
        """
        all_pages = []
        for range_str in page_ranges:
            all_pages.extend(PageRangeParser.parse_range(range_str))
        return sorted(list(set(all_pages)))

    @staticmethod
    def group_consecutive_pages(pages_list: List[int]) -> List[str]:
        """
        Regroupe les pages consécutives en plages
        
        Args:
            pages_list: Liste de numéros de pages
            
        Returns:
            Liste de plages (ex: ["1-3", "5-5", "7-9"])
        """
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
        
        # Ajouter la dernière plage
        if debut == fin:
            plages.append(f"{debut}-{debut}")
        else:
            plages.append(f"{debut}-{fin}")
        
        return plages


class OutputCapture:
    """Utilitaire pour capturer les sorties console"""
    
    @staticmethod
    def capture_prints(func: Callable, *args, **kwargs) -> Tuple[Any, str]:
        """
        Capture les prints d'une fonction
        
        Args:
            func: Fonction à exécuter
            *args: Arguments de la fonction
            **kwargs: Arguments nommés de la fonction
            
        Returns:
            Tuple (résultat_fonction, sortie_capturée)
        """
        old_stdout = sys.stdout
        captured_output = io.StringIO()
        
        try:
            sys.stdout = captured_output
            result = func(*args, **kwargs)
            output = captured_output.getvalue()
            return result, output
        finally:
            sys.stdout = old_stdout


class ColumnCleaner:
    """Utilitaire pour nettoyer les noms de colonnes"""
    
    @staticmethod
    def clean_column_names(columns: List[str], verbose: bool = True) -> List[str]:
        """
        Nettoie les noms de colonnes en supprimant les caractères spéciaux
        
        Args:
            columns: Liste des noms de colonnes
            verbose: Afficher les colonnes modifiées
            
        Returns:
            Liste des noms nettoyés
        """
        cleaned_columns = []
        
        for i, col in enumerate(columns):
            col_str = str(col)
            
            # Détecter les caractères spéciaux
            if verbose and ('\n' in col_str or '\r' in col_str or '\t' in col_str):
                print(f"    🔧 Colonne {i} contient des caractères spéciaux: {repr(col_str)}")
            
            # Nettoyer les caractères spéciaux
            cleaned_col = col_str.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            # Nettoyer les espaces multiples
            cleaned_col = re.sub(r'\s+', ' ', cleaned_col)
            # Supprimer les espaces en début et fin
            cleaned_col = cleaned_col.strip()
            
            cleaned_columns.append(cleaned_col)
        
        return cleaned_columns

    @staticmethod
    def handle_unnamed_columns(columns: List[str], df_data: Any) -> Tuple[List[str], bool]:
        """
        Gère les colonnes sans nom ou "Unnamed"
        
        Args:
            columns: Liste des noms de colonnes
            df_data: Données du DataFrame pour récupérer les valeurs
            
        Returns:
            Tuple (nouvelles_colonnes, faut_il_supprimer_premiere_ligne)
        """
        # Créer le masque des colonnes sans nom
        mask_unnamed = [
            (c is None) or (isinstance(c, str) and (c.strip() == "" or c.lower().startswith("unnamed:")))
            for c in columns
        ]
        
        if not any(mask_unnamed):
            return columns, False
        
        new_cols = []
        should_remove_first_row = False
        
        for i, c in enumerate(columns):
            if mask_unnamed[i]:
                should_remove_first_row = True
                left_name = new_cols[i-1] if i > 0 else "col0"
                
                # Essayer de récupérer les valeurs pour nommer la colonne
                try:
                    first_val = df_data.iloc[0, i] if hasattr(df_data, 'iloc') and len(df_data) > 0 else ""
                    left_first_val = df_data.iloc[0, i-1] if i > 0 and hasattr(df_data, 'iloc') and len(df_data) > 0 else ""
                    
                    # Mettre à jour la colonne précédente
                    if i > 0:
                        new_cols[i-1] = f"{left_name}_{str(left_first_val).strip()}"
                    
                    new_cols.append(f"{left_name}_{str(first_val).strip()}")
                except:
                    new_cols.append(f"col_{i}")
            else:
                new_cols.append(str(c))
        
        return new_cols, should_remove_first_row


def format_page_ranges(pages_list: List[int]) -> str:
    """
    Formate une liste de pages en plages lisibles
    
    Args:
        pages_list: Liste de numéros de pages
        
    Returns:
        String formaté des plages
    """
    if not pages_list:
        return "Aucune"
    
    ranges = PageRangeParser.group_consecutive_pages(pages_list)
    return ", ".join(ranges)


def safe_int_conversion(value: Any) -> int:
    """
    Conversion sécurisée vers un entier
    
    Args:
        value: Valeur à convertir
        
    Returns:
        Entier ou 0 si conversion impossible
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

def debug_print(message: str, level: str = "INFO"):
    """
    Fonction de debug pour identifier les problèmes
    
    Args:
        message: Message à afficher
        level: Niveau de log
    """
    import datetime
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {level}: {message}")
