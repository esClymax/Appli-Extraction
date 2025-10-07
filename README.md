## 📊 Extracteur Multi-PDF vers CSV Global

Une application Streamlit pour extraire des tableaux de fichiers PDF et les convertir en fichiers CSV consolidés.

## ✨ Fonctionnalités

- **Extraction multi-PDF** : Traite plusieurs fichiers PDF en une seule fois
- **CSV individuels** : Un fichier CSV par PDF
- **CSV global consolidé** : Un fichier unique contenant toutes les données de tous les PDF
- **Interface web intuitive** : Interface Streamlit facile à utiliser et suivi en temps réel

## 🏗️ Structure des données

### CSV Global

- **Colonne 1** : "Document" (nom du PDF d'origine)
- **Colonne 2** : "Catégorie" (type de bordereau)  
- **Colonne 3** : "Nom & Prénom"
- **Autres colonnes** : Données spécifiques à chaque catégorie

## 🚀 Installation

### Prérequis
- Python 3.8 ou plus récent
- pip (gestionnaire de packages Python)

### Installation locale

Télécharger le dossier .zip et l'extraire  
Taper les commandes suivantes : 

```bash
pip install -r requirements.txt
streamlit run webApp.py
