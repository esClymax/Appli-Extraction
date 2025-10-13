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
                     pdf_filename: str) -> pd.DataFrame:  # Supprimé Optional
        """
        Traite complètement un DataFrame - GARANTIT de retourner un DataFrame
        """
        # Cas d'entrée None ou vide
        if df is None or df.empty:
            print(f"    ⚠️ DataFrame None ou vide pour {category_name} - création DataFrame minimal")
            empty_df = pd.DataFrame({
                'Document': [pdf_filename.replace('.pdf', '')],
                'Catégorie': [DICO_BORDEREAU.get(category_name, category_name)],
                'Nom & Prénom': ['']
            })
            return empty_df
        
        try:
            print(f"    🔧 Traitement DataFrame: {len(df)} lignes, {len(df.columns)} colonnes")
            
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
            
            # Si le DataFrame devient vide après nettoyage
            if df.empty:
                print(f"    ⚠️ DataFrame vide après nettoyage - création ligne minimale")
                df = pd.DataFrame({
                    'temp_col': ['Aucune donnée valide']
                })
            
            # Ajouter les colonnes métadonnées
            df = self._add_metadata_columns(df, category_name, pdf_filename)
            
            # Standardiser la colonne nom
            df = self._standardize_name_column(df)
            
            print(f"    ✅ Traitement terminé: {len(df)} lignes, {len(df.columns)} colonnes")
            return df
            
        except Exception as e:
            print(f"    ❌ Erreur traitement DataFrame: {e}")
            # Créer un DataFrame d'erreur au lieu de retourner None
            error_df = pd.DataFrame({
                'Document': [pdf_filename.replace('.pdf', '')],
                'Catégorie': [DICO_BORDEREAU.get(category_name, category_name)],
                'Nom & Prénom': [f'Erreur: {str(e)}']
            })
            return error_df

    
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
    """Combinateur de DataFrames avec gestion robuste des index"""
    
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
            # S'assurer que l'index est propre même pour un seul DataFrame
            return tables[0].reset_index(drop=True)
        
        try:
            # Réinitialiser les index de tous les DataFrames avant concaténation
            clean_tables = []
            for i, table in enumerate(tables):
                if table is not None and not table.empty:
                    # Réinitialiser l'index pour éviter les conflits
                    clean_table = table.reset_index(drop=True)
                    clean_tables.append(clean_table)
                    print(f"      Table {i+1}: {len(clean_table)} lignes préparées")
            
            if not clean_tables:
                return pd.DataFrame()
            
            # Concaténation sécurisée
            result = pd.concat(clean_tables, ignore_index=True, sort=False)
            print(f"      ✅ {len(clean_tables)} tableaux combinés -> {len(result)} lignes")
            return result
            
        except Exception as e:
            print(f"⚠️ Erreur combinaison tableaux: {e}")
            # En cas d'erreur, retourner le plus grand tableau, nettoyé
            if tables:
                largest_table = max(tables, key=lambda x: len(x) if x is not None else 0)
                return largest_table.reset_index(drop=True) if largest_table is not None else pd.DataFrame()
            return pd.DataFrame()
    
@staticmethod
def concatenate_all_dataframes(dataframes_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatène tous les DataFrames - GARANTIT de retourner un DataFrame (jamais None)
    """
    print(f"🔗 Début concaténation de {len(dataframes_list)} DataFrames...")
    
    # Cas de base : liste vide
    if not dataframes_list:
        print("   📋 Liste vide - retour DataFrame vide")
        return pd.DataFrame(columns=['Document', 'Catégorie', 'Nom & Prénom'])
    
    # Filtrer les DataFrames valides
    valid_dataframes = []
    for i, df in enumerate(dataframes_list):
        if df is not None and not df.empty:
            valid_dataframes.append(df)
            print(f"   ✅ DataFrame {i+1}: {len(df)} lignes accepté")
        else:
            print(f"   ❌ DataFrame {i+1}: None ou vide - ignoré")
    
    # Cas : aucun DataFrame valide
    if not valid_dataframes:
        print("   📋 Aucun DataFrame valide - retour DataFrame vide")
        return pd.DataFrame(columns=['Document', 'Catégorie', 'Nom & Prénom'])
    
    # Cas : un seul DataFrame valide
    if len(valid_dataframes) == 1:
        result = valid_dataframes[0].reset_index(drop=True)
        result = DataFrameCombiner._ensure_basic_columns(result)
        print(f"   📋 Un seul DataFrame - retour {len(result)} lignes")
        return result
    
    # Cas : plusieurs DataFrames à concaténer
    try:
        print(f"   🔗 Concaténation de {len(valid_dataframes)} DataFrames...")
        
        # Nettoyer et préparer chaque DataFrame
        clean_dataframes = []
        for i, df in enumerate(valid_dataframes):
            # Diagnostics
            print(f"     DataFrame {i+1}: {len(df)} lignes, {len(df.columns)} colonnes")
            print(f"     Index unique: {df.index.is_unique}")
            
            # Nettoyer le DataFrame
            clean_df = df.copy().reset_index(drop=True)
            
            # Gérer les colonnes dupliquées
            if clean_df.columns.duplicated().any():
                print(f"     ⚠️ Colonnes dupliquées détectées - correction")
                clean_df.columns = DataFrameCombiner._make_unique_columns(clean_df.columns)
            
            clean_dataframes.append(clean_df)
        
        # Concaténation sécurisée
        merged_df = pd.concat(
            clean_dataframes, 
            ignore_index=True,
            sort=False,
            copy=True
        )
        
        print(f"   ✅ Concaténation réussie: {len(merged_df)} lignes totales")
        
        # Post-traitement
        merged_df = DataFrameCombiner._reorder_columns(merged_df)
        merged_df = merged_df.fillna('')
        merged_df = DataFrameCombiner._ensure_basic_columns(merged_df)
        
        return merged_df
        
    except Exception as e:
        print(f"   ❌ Erreur lors de la concaténation: {e}")
        print(f"   🔄 Tentative de récupération...")
        
        # Méthode de récupération garantie
        try:
            result = DataFrameCombiner._emergency_concatenation(valid_dataframes)
            print(f"   ✅ Récupération réussie: {len(result)} lignes")
            return result
        except Exception as e2:
            print(f"   ❌ Échec récupération: {e2}")
            # Dernière chance : retourner le plus grand DataFrame
            largest = max(valid_dataframes, key=len)
            result = largest.reset_index(drop=True)
            result = DataFrameCombiner._ensure_basic_columns(result)
            print(f"   🆘 Retour du plus grand DataFrame: {len(result)} lignes")
            return result

@staticmethod
def _ensure_basic_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    S'assure que le DataFrame a au minimum les colonnes de base
    """
    basic_columns = ['Document', 'Catégorie', 'Nom & Prénom']
    
    for col in basic_columns:
        if col not in df.columns:
            df[col] = ''
    
    return df

@staticmethod
def _emergency_concatenation(dataframes_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Méthode de concaténation d'urgence - ligne par ligne
    """
    if not dataframes_list:
        return pd.DataFrame(columns=['Document', 'Catégorie', 'Nom & Prénom'])
    
    # Commencer avec le premier DataFrame
    result = dataframes_list[0].copy().reset_index(drop=True)
    
    # Ajouter les autres un par un
    for df in dataframes_list[1:]:
        df_clean = df.copy().reset_index(drop=True)
        
        # Harmoniser les colonnes
        all_columns = list(set(result.columns) | set(df_clean.columns))
        
        # Ajouter les colonnes manquantes
        for col in all_columns:
            if col not in result.columns:
                result[col] = ''
            if col not in df_clean.columns:
                df_clean[col] = ''
        
        # Réorganiser dans le même ordre
        result = result[all_columns]
        df_clean = df_clean[all_columns]
        
        # Concaténer
        result = pd.concat([result, df_clean], ignore_index=True)
    
    return DataFrameCombiner._ensure_basic_columns(result)

    
    @staticmethod
    def _make_unique_columns(columns):
        """Rend les noms de colonnes uniques"""
        seen = {}
        unique_cols = []
        
        for col in columns:
            if col in seen:
                seen[col] += 1
                unique_cols.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                unique_cols.append(col)
        
        return unique_cols
    
    @staticmethod
    def _fallback_concatenation(dataframes_list: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Méthode de récupération en cas d'échec de la concaténation normale
        """
        print("🔄 Tentative de récupération avec méthode alternative...")
        
        try:
            # Méthode 1: Concaténation une par une
            result_df = dataframes_list[0].copy().reset_index(drop=True)
            
            for i, df in enumerate(dataframes_list[1:], 1):
                if df is not None and not df.empty:
                    df_clean = df.copy().reset_index(drop=True)
                    
                    # Ajouter les colonnes manquantes
                    for col in df_clean.columns:
                        if col not in result_df.columns:
                            result_df[col] = ''
                    
                    for col in result_df.columns:
                        if col not in df_clean.columns:
                            df_clean[col] = ''
                    
                    # Réorganiser les colonnes dans le même ordre
                    df_clean = df_clean[result_df.columns]
                    
                    # Concaténer
                    result_df = pd.concat([result_df, df_clean], ignore_index=True)
                    print(f"   Ajouté DataFrame {i}: {len(result_df)} lignes totales")
            
            return result_df
            
        except Exception as e2:
            print(f"❌ Échec méthode de récupération: {e2}")
            # Dernière chance: retourner le plus grand DataFrame
            if dataframes_list:
                largest = max(dataframes_list, key=lambda x: len(x) if x is not None else 0)
                return largest.reset_index(drop=True) if largest is not None else pd.DataFrame()
            return pd.DataFrame()
    
    @staticmethod
    def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Réorganise les colonnes avec Document, Catégorie, Nom & Prénom en premier"""
        if df.empty:
            return df
            
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
