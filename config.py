"""
Configuration et constantes de l'application
"""

# Dictionnaire des bordereaux
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

# Mots-clés pour l'extraction
MOTS_CLES = [
    "Bordereau A1 n", "Bordereau I2 n", "Bordereau A3 n", 
    "Bordereau A4 n", "Bordereau A5 n", "Bordereau A50 n", 
    "Bordereau A6 n", "Bordereau A6 bis n", "Bordereau A7 n", 
    "Bordereau A7 bis n", "Bordereau A7 ter n", "Bordereau I8 n", 
    "Bordereau A9 n"
]

# Configuration Streamlit
STREAMLIT_CONFIG = {
    "page_title": "Extracteur Multi-PDF vers CSV Global",
    "page_icon": "📊",
    "layout": "wide"
}

# Configuration par défaut pour l'extraction
DEFAULT_CLEANING_RULES = {
    'remove_empty_rows': True,
    'remove_empty_columns': True,
    'strip_whitespace': True,
}
