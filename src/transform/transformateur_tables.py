"""
SYSTÈME DE TRANSFORMATION DE DONNÉES
=====================================
Architecture modulaire pour nettoyer et transformer des tables de données
avec support de traitements spécialisés et génériques.

Architecture:
    - TransformateurParDefaut: Nettoyage standard pour toutes les tables
    - Transformateurs Spécialisés: Logique métier pour tables critiques
    - REGISTRE_TRANSFORMATIONS: Mappage table → transformateur
    - transformer_table: Chef d'orchestre principal
"""

import pandas as pd
import yaml
import numpy as np
from typing import Tuple, Dict, Any
import warnings
from datetime import datetime


# ============================================================================
# CLASSE 1: TRANSFORMATEUR PAR DÉFAUT (Robot de nettoyage standard)
# ============================================================================

class TransformateurParDefaut:
    """
    Robot standard qui nettoie toute table inconnue.
    
    Responsabilités:
        - Normaliser les noms de colonnes
        - Convertir les dates
        - Supprimer les doublons
        - Gérer les valeurs manquantes
        - Produire des métadonnées de traçabilité
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialise le transformateur avec sa configuration.
        
        Args:
            config: Dictionnaire de configuration (chargé depuis YAML)
        """
        self.config = config
    
    def transformer(self, df: pd.DataFrame, table_name: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Applique le nettoyage standard et retourne les métadonnées.
        
        Args:
            df: DataFrame pandas à transformer
            table_name: Nom de la table (pour traçabilité)
            
        Returns:
            Tuple contenant:
                - DataFrame nettoyé
                - Dictionnaire de métadonnées (lignes modifiées, colonnes, etc.)
        """
        
        # ----------------------------------------------------------------
        # INITIALISATION: Préparer le dictionnaire de métadonnées
        # ----------------------------------------------------------------
        metadata = {
            "table": table_name,
            "traitement": "STANDARD",
            "lignes_avant": len(df),           # Nombre de lignes avant nettoyage
            "colonnes_avant": len(df.columns)  # Nombre de colonnes avant nettoyage
        }
        
        # ----------------------------------------------------------------
        # ÉTAPE 1: copie de securité
        # ----------------------------------------------------------------
        df_clean = df.copy()

        # ----------------------------------------------------------------
        # ÉTAPE 2: Normalisation des noms de colonnes
        # ----------------------------------------------------------------
        colonnes_originales = df_clean.columns.tolist()

        ## chaine de transformation 
        df_clean.columns = [
            df_clean.columns.
            str.lower().        ## tout en minuscule (les noms de colonnes)
            str.replace(' ', '_').      ## espaces remplacés par des underscores
            str.replace('-', '_').      ## tirets remplacés par des underscores
            str.replace(r'[^\w\s]', '', regex=True) ## caractères spéciaux supprimés
        ]

        ## enregistrer le mapping des colonnes ancien nom -> nouveau nom dans le metadata
        metadata["colonnes_renommees"] = dict(zip(colonnes_originales, df_clean.columns))

         # ----------------------------------------------------------------
        # ÉTAPE 3: CONVERTIR LES COLONNES DE TYPE DATE
        # ----------------------------------------------------------------
        # Objectif: Détecter et convertir automatiquement les colonnes contenant des dates

        dates_converties = []       ## ligne pour tracker les colonnes non converties
        
        ## parcourir toutes les colonnes pour détecter les dates
        for col in df_clean.columns:
            #condition le nom contient 'date' ou 'jour' (indicateur simple) et le type est object (texte)
            if (('date' in col) or ('jour' in col)) and df_clean[col].dtype == 'object':
                try:
                     # Convertir en datetime pandas
                    # errors='coerce': valeurs invalides → NaT (Not a Time) au lieu d'erreur
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                    dates_converties.append(col)  # Succès: ajouter à la liste
                except:
                    # En cas d'échec, on ignore silencieusement (colonne reste inchangée)
                    pass
        
        ## enregistrer les colonnes converties dans le metadata
        metadata["dates_converties"] = dates_converties

        # ----------------------------------------------------------------
        # ÉTAPE 4: SUPPRIMER LES DOUBLONS EXACTS
        # ----------------------------------------------------------------

        ## eliminer les doublons (ligne identiques)

        # Vérifier si la suppression est activée dans la config
        # Par défaut: True si non spécifié
        if self.config.get("regles_default", {}).get("supprimer_doublons", True):

            ## compter les lignes avant suppression
            lignes_avant = len(df_clean)

            ## supprimer les doublons (garde la première occurrence)
            df_clean = df_clean.drop_duplicates(keep='first').reset_index(drop=True)

            ## compter les lignes après suppression
            metadata["lignes_supprimees_doublons"] = lignes_avant - len(df_clean)

         # ----------------------------------------------------------------
        # ÉTAPE 5: REMPLIR LES VALEURS MANQUANTES
        # ----------------------------------------------------------------
        # Objectif: Remplacer tous les NaN/None par une valeur par défaut
        
        # Récupérer la valeur de remplacement depuis la config
        # Par défaut: "INCONNU" si non spécifié
        valeur_manquante = self.config.get("regles_default",{}).get("valeur_manquante", "INCONNU")

        ## Remplacer les valeurs manquantes dans tout le DataFrame
        df_clean = df_clean.fillna(valeur_manquante)

        ## enrgistrer dans le metadata
        metadata["valeur_manquante_remplacement"] = valeur_manquante

        # ----------------------------------------------------------------
        # ÉTAPE 6: FINALISATION ET MÉTADONNÉES
        # ----------------------------------------------------------------
        metadata["lignes_apres"] = len(df_clean)           # Nombre de lignes après nettoyage
        metadata["colonnes_apres"] = len(df_clean.columns)  # Nombre de colonnes après nettoyage
        metadata["colonnes_finale"] = df_clean.columns.tolist()  # Liste des colonnes finales

        ## Retourner le DataFrame nettoyé et les métadonnées
        return df_clean, metadata