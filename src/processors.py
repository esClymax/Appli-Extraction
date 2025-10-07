"""
Processeurs pour nettoyer et traiter les données extraites
"""

import pandas as pd
import re
from typing import Dict, Any, List, Optional
from .utils import ColumnCleaner
from config import NAME_COLUMN_PATTERNS, DICO_BORDEREAU


class DataCleaner:
    """Nettoyeur de données avec règles configurables"""
    
    def __init__(self, cleaning_rules: Dict[str, Any]):
        self.rules = cleaning_rules
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie un DataFrame selon les règles configurées
        
        Args:
            df: DataFrame à nettoyer
            
        Returns:
            DataFrame nettoyé
        """
        df_clean = df.copy()
        
        # Supprimer les lignes vides
        if self.rules.get('remove_empty_rows', True):
            df_clean = df_clean.dropna(how='all')
        
        # Supprimer les colonnes vides
        if self.rules.get('remove_empty_columns', True):
            df_clean = df_clean.dropna(axis=1, how='all')
        
        # Nettoyer les espaces
        if self.rules.get('strip_whitespace', True):
            df_clean = self._strip_whitespace(df_clean)
        
        # Appliquer les règles regex
        df_clean = self._apply_regex_rules(df_clean)
        
        return df_clean
    
    def _strip_whitespace(self, df: pd.DataFrame) -> pd.DataFrame:
        """Supprime les espaces en début/fin des cellules"""
        return df.map(lambda x: x.strip() if isinstance(x, str) else x)
    
    def _apply_regex_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applique les règles regex configurées"""
        regex_rules = self.rules.get('regex_patterns', {})
        
        for column, patterns in regex_rules.items():
            if column in df.columns:
                for pattern, replacement in patterns.items():
                    df[column] = df[column].astype(str).str.replace(
                        pattern, replacement, regex=True
                    )
        
        return df


class DataFrameProcessor:
    """Processeur principal pour les DataFrames"""
    
    def __init__(self, cleaning_rules: Dict[str, Any]):
        self.cleaner = DataCleaner(cleaning_rules)
    
    def process_dataframe(self, df: pd.DataFrame, category_name: str,
                         pdf_filename: str) -> Optional[pd.DataFrame]:
        """
        Traite complètement un DataFrame
        
        Args:
            df: DataFrame à traiter
            category_name: Nom de la catégorie
            pdf_filename: Nom du fichier PDF
            
        Returns:
            DataFrame traité ou None si erreur
        """
        if df is None or df.empty:
            return None
        
        try:
            # Nettoyer les données
            df = self.cleaner.clean_dataframe(df)
            
            # Gérer les colonnes sans nom
            df = self._handle_unnamed_columns(df)
            
            # Nettoyer les noms de colonnes
            df = self._clean_column_names(df)
            
            # Traitement spécifique par catégorie
            df = self._apply_category_specific_processing(df, category_name)
            
            # Supprimer les lignes vides
            df = self._remove_empty_rows(df)
            
            # Ajouter les colonnes métadonnées
            df = self._add_metadata_columns(df, category_name, pdf_filename)
            
            # Standardiser la colonne nom
            df = self._standardize_name_column(df)
            
            return df
            
        except Exception as e:
            print(f"❌ Erreur traitement DataFrame: {e}")
            return None
    
    def _handle_unnamed_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gère les colonnes sans nom ou 'Unnamed'"""
        new_cols, should_remove_first_row = ColumnCleaner.handle_unnamed_columns(
            list(df.columns), df
        )
        
        if should_remove_first_row:
            df = df.iloc[1:].reset_index(drop=True)
        
        df.columns = new_cols
        return df
    
    def _clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Nettoie les noms de colonnes"""
        cleaned_columns = ColumnCleaner.clean_column_names(list(df.columns))
        df.columns = cleaned_columns
        return df
    
    def _apply_category_specific_processing(self, df: pd.DataFrame,
                                          category_name: str) -> pd.DataFrame:
        """Applique un traitement spécifique selon la catégorie"""
        
        if category_name == "Bordereau A5 n":
            # Traitement spécial pour Bordereau A5
            df = self._process_bordereau_a5(df)
        
        # Ajouter d'autres traitements spécifiques ici
        
        return df
    
    def _process_bordereau_a5(self, df: pd.DataFrame) -> pd.DataFrame:
        """Traitement spécifique pour le Bordereau A5"""
        if len(df.columns) > 5:
            try:
                # Correspondance exacte pour "aucune candidature"
                mask = df.iloc[:, 5].str.lower().str.strip() == 'aucune candidature'
                
                # Transférer vers la 1ère colonne
                df.loc[mask, df.columns[0]] = 'aucune candidature'
                
                # Vider la 5ème colonne pour ces lignes
                df.loc[mask, df.columns[5]] = ''
            except Exception as e:
                print(f"⚠️ Erreur traitement Bordereau A5: {e}")
        
        return df
    
    def _remove_empty_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Supprime les lignes vides ou non pertinentes"""
        if len(df.columns) > 0:
            mask = (df.iloc[:, 0].astype(str).str.strip() != '')
            df = df[mask].reset_index(drop=True)
        return df
    
    def _add_metadata_columns(self, df: pd.DataFrame, category_name: str,
                             pdf_filename: str) -> pd.DataFrame:
        """Ajoute les colonnes de métadonnées"""
        # Ajouter la colonne Document
        document_name = pdf_filename.replace('.pdf', '') if pdf_filename.endswith('.pdf') else pdf_filename
        df.insert(0, 'Document', document_name)
        
        # Ajouter la colonne Catégorie
        category_label = DICO_BORDEREAU.get(category_name, category_name)
        df.insert(1, 'Catégorie', category_label)
        
        return df
    
    def _standardize_name_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardise le nom de la colonne contenant les noms et prénoms"""
        
        for col in df.columns:
            col_lower = str(col).lower()
            for pattern in NAME_COLUMN_PATTERNS:
                if re.search(pattern, col_lower):
                    df = df.rename(columns={col: 'Nom & Prénom'})
                    return df
        
        # Si aucune colonne trouvée, prendre la première après Document et Catégorie
        if len(df.columns) > 2:
            df = df.rename(columns={df.columns[2]: 'Nom & Prénom'})
        
        return df


class DataFrameCombiner:
    """Combinateur de DataFrames"""
    
    @staticmethod
    def combine_tables(tables: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine plusieurs tableaux d'une même catégorie
        
        Args:
            tables: Liste des DataFrames à combiner
            
        Returns:
            DataFrame combiné
        """
        if len(tables) == 1:
            return tables[0]
        
        try:
            return pd.concat(tables, ignore_index=True, sort=False)
        except Exception as e:
            print(f"⚠️ Erreur combinaison tableaux: {e}")
            # En cas d'erreur, retourner le plus grand tableau
            return max(tables, key=len) if tables else pd.DataFrame()
    
    @staticmethod
    def concatenate_all_dataframes(dataframes_list: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Concatène tous les DataFrames en préservant toutes les lignes
        
        Args:
            dataframes_list: Liste des DataFrames à concaténer
            
        Returns:
            DataFrame concaténé
        """
        if len(dataframes_list) == 1:
            return dataframes_list[0]
        
        # Concaténer tous les DataFrames
        merged_df = pd.concat(dataframes_list, ignore_index=True, sort=False)
        
        # Réorganiser les colonnes
        merged_df = DataFrameCombiner._reorder_columns(merged_df)
        
        # Remplir les valeurs manquantes
        merged_df = merged_df.fillna('')
        
        return merged_df
    
    @staticmethod
    def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Réorganise les colonnes avec Document, Catégorie, Nom & Prénom en premier"""
        cols_to_front = []
        
        # Colonnes prioritaires dans l'ordre
        priority_cols = ['Document', 'Catégorie', 'Nom & Prénom']
        
        for col in priority_cols:
            if col in df.columns:
                cols_to_front.append(col)
        
        # Ajouter les autres colonnes
        remaining_cols = [col for col in df.columns if col not in cols_to_front]
        final_columns_order = cols_to_front + remaining_cols
        
        return df[final_columns_order]


class FilterProcessor:
    """Processeur pour appliquer des filtres aux données"""
    
    @staticmethod
    def apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        Applique des filtres configurés au DataFrame
        
        Args:
            df: DataFrame à filtrer
            filters: Configuration des filtres
            
        Returns:
            DataFrame filtré
        """
        filtered_df = df.copy()
        
        for column, filter_config in filters.items():
            if column not in filtered_df.columns:
                continue
            
            filter_type = filter_config.get('type', 'contains')
            filter_value = filter_config.get('value', '')
            
            try:
                if filter_type == 'contains':
                    mask = filtered_df[column].astype(str).str.contains(
                        filter_value, na=False, case=False
                    )
                    filtered_df = filtered_df[mask]
                    
                elif filter_type == 'equals':
                    filtered_df = filtered_df[filtered_df[column] == filter_value]
                    
                elif filter_type == 'not_empty':
                    filtered_df = filtered_df[filtered_df[column].notna()]
                    
                elif filter_type == 'regex':
                    mask = filtered_df[column].astype(str).str.match(
                        filter_value, na=False
                    )
                    filtered_df = filtered_df[mask]
                    
            except Exception as e:
                print(f"⚠️ Erreur application filtre {column}: {e}")
                continue
        
        return filtered_df
