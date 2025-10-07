"""
Configuration de l'extracteur PDF
"""

# Dictionnaire des bordereaux avec leurs libellés
DICO_BORDEREAU = {
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

# Mots-clés à rechercher dans les PDF
MOTS_CLES = [
    "Bordereau A1 n", "Bordereau I2 n", "Bordereau A3 n",
    "Bordereau A4 n", "Bordereau A5 n", "Bordereau A50 n",
    "Bordereau A6 n", "Bordereau A6 bis n", "Bordereau A7 n",
    "Bordereau A7 bis n", "Bordereau A7 ter n", "Bordereau I8 n",
    "Bordereau A9 n"
]

# Configuration par défaut pour le nettoyage des données
DEFAULT_CLEANING_RULES = {
    'remove_empty_rows': True,
    'remove_empty_columns': True,
    'strip_whitespace': True,
}

# Patterns pour identifier les colonnes de noms
NAME_COLUMN_PATTERNS = [
    r'nom.*pr[eé]nom',
    r'pr[eé]nom.*nom',
    r'^nom$',
    r'nom',
    r'pr[eé]nom',
    r'identit[eé]',
    r'personne'
]

# Configuration Streamlit
STREAMLIT_CONFIG = {
    'page_title': 'Extracteur Multi-PDF vers CSV Global',
    'page_icon': '📊',
    'layout': 'wide'
}

# Colonnes spéciales pour Bordereau A5
BORDEREAU_A5_COLUMNS = [
    'UM', 'DUM', 'SDUM', 
    'Emploi_Lieu_de_travail_Publié_sous_le',
    'Nombre_demploi_Lieu_de_travail_Date_de_forclusion',
    'Motif_Position_GF_de_publication',
    'CERNE_Reference_My_HR'
]
