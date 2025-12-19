"""
Classe TransformateurParDefaut
Applique des transformations génériques sur n'importe quelle table.
"""
import pandas as pd
import numpy as np
import re
from datetime import datetime
from typing import Dict, Any, Tuple
import unicodedata
from src.config.logging_config import setup_logger

logger = setup_logger(__name__)


class TransformateurParDefaut:
    """
    Transformateur générique pour toute table inconnue.
    
    Applique automatiquement :
    - Normalisation des noms de colonnes
    - Suppression des doublons
    - Conversion automatique des dates
    - Remplissage des valeurs manquantes
    - Génération de métadonnées
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialise le transformateur avec la configuration.
        
        Args:
            config: Dictionnaire de configuration (section regles_default du YAML)
        """
        self.config = config
        self.metadata = {
            'timestamp_debut': datetime.now(),
            'warnings': [],
            'colonnes_avant': [],
            'colonnes_apres': [],
            'transformations_appliquees': []
        }
    
    def transformer(self, df: pd.DataFrame, nom_table: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Applique toutes les transformations par défaut.
        
        Args:
            df: DataFrame à transformer
            nom_table: Nom de la table (pour logging)
        
        Returns:
            Tuple (DataFrame transformé, métadonnées)
        """
        logger.info(f"="*60)
        logger.info(f"🔄 TRANSFORMATION PAR DÉFAUT : {nom_table}")
        logger.info(f"="*60)
        
        # Sauvegarder état initial
        self.metadata['nombre_lignes_avant'] = len(df)
        self.metadata['colonnes_avant'] = df.columns.tolist()
        
        df_transforme = df.copy()
        
        # Étape 1: Normaliser les noms de colonnes
        if self.config.get('normaliser_colonnes', True):
            df_transforme = self._normaliser_colonnes(df_transforme)
        
        # Étape 2: Supprimer les doublons exacts
        if self.config.get('supprimer_doublons', True):
            df_transforme = self._supprimer_doublons(df_transforme)
        
        # Étape 3: Détecter et convertir les dates
        if self.config.get('convertir_dates_auto', True):
            df_transforme = self._convertir_dates_auto(df_transforme)
        
        # Étape 4: Remplir les valeurs manquantes
        valeur_defaut = self.config.get('valeur_manquante_defaut', 'INCONNU')
        df_transforme = self._remplir_valeurs_manquantes(df_transforme, valeur_defaut)
        
        # Étape 5: Détecter colonnes sensibles
        self._detecter_colonnes_sensibles(df_transforme)
        
        # Métadonnées finales
        self.metadata['nombre_lignes_apres'] = len(df_transforme)
        self.metadata['colonnes_apres'] = df_transforme.columns.tolist()
        self.metadata['colonnes_ajoutees'] = list(
            set(self.metadata['colonnes_apres']) - set(self.metadata['colonnes_avant'])
        )
        self.metadata['colonnes_supprimees'] = list(
            set(self.metadata['colonnes_avant']) - set(self.metadata['colonnes_apres'])
        )
        self.metadata['timestamp_fin'] = datetime.now()
        self.metadata['duree_execution'] = (
            self.metadata['timestamp_fin'] - self.metadata['timestamp_debut']
        ).total_seconds()
        
        # Calculer taux de valeurs manquantes
        self.metadata['taux_valeurs_manquantes'] = (
            df_transforme.isnull().sum().sum() / (len(df_transforme) * len(df_transforme.columns))
        ) * 100
        
        logger.info(f"✅ Transformation terminée en {self.metadata['duree_execution']:.2f}s")
        logger.info(f"📊 {self.metadata['nombre_lignes_avant']} → {self.metadata['nombre_lignes_apres']} lignes")
        
        return df_transforme, self.metadata
    
    def _normaliser_colonnes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalise les noms de colonnes :
        - Minuscules
        - Remplace espaces par underscore
        - Supprime accents
        - Supprime caractères spéciaux
        """
        logger.info("🔤 Normalisation des noms de colonnes...")
        
        colonnes_normalisees = {}
        for col in df.columns:
            # Supprimer les accents
            col_sans_accent = ''.join(
                c for c in unicodedata.normalize('NFD', str(col))
                if unicodedata.category(c) != 'Mn'
            )
            
            # Minuscules, remplacer espaces et caractères spéciaux
            col_propre = re.sub(r'[^a-z0-9_]', '_', col_sans_accent.lower())
            col_propre = re.sub(r'_+', '_', col_propre)  # Supprimer underscores multiples
            col_propre = col_propre.strip('_')  # Supprimer underscores début/fin
            
            colonnes_normalisees[col] = col_propre
            
            if col != col_propre:
                logger.debug(f"  '{col}' → '{col_propre}'")
        
        df_renomme = df.rename(columns=colonnes_normalisees)
        self.metadata['transformations_appliquees'].append('normalisation_colonnes')
        
        return df_renomme
    
    def _supprimer_doublons(self, df: pd.DataFrame) -> pd.DataFrame:
        """Supprime les doublons exacts (toutes colonnes identiques)"""
        nb_avant = len(df)
        df_dedup = df.drop_duplicates(keep='first')
        nb_doublons = nb_avant - len(df_dedup)
        
        if nb_doublons > 0:
            logger.warning(f"⚠️  {nb_doublons} doublons exacts supprimés ({nb_doublons/nb_avant*100:.1f}%)")
            self.metadata['doublons_supprimes'] = nb_doublons
        else:
            logger.info("✓ Aucun doublon détecté")
            self.metadata['doublons_supprimes'] = 0
        
        self.metadata['transformations_appliquees'].append('suppression_doublons')
        return df_dedup
    
    def _convertir_dates_auto(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Détecte et convertit automatiquement les colonnes de dates.
        Critère : nom de colonne contient 'date', 'time', ou 'datetime'
        """
        logger.info("📅 Détection automatique des dates...")
        
        colonnes_dates = [col for col in df.columns if any(
            mot in col.lower() for mot in ['date', 'time', 'datetime', 'timestamp']
        )]
        
        for col in colonnes_dates:
            try:
                # Tentative de conversion
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.info(f"  ✓ '{col}' converti en datetime")
                self.metadata['transformations_appliquees'].append(f'conversion_date_{col}')
            
            except Exception as e:
                logger.warning(f"  ⚠️  Impossible de convertir '{col}' : {str(e)}")
        
        return df
    
    def _remplir_valeurs_manquantes(self, df: pd.DataFrame, valeur_defaut: str) -> pd.DataFrame:
        """
        Remplit les valeurs manquantes avec une valeur par défaut.
        RÈGLE : Seulement pour colonnes texte (object)
        """
        logger.info(f"🔧 Remplissage des valeurs manquantes avec '{valeur_defaut}'...")
        
        colonnes_texte = df.select_dtypes(include=['object']).columns
        nb_nulls_avant = df[colonnes_texte].isnull().sum().sum()
        
        if nb_nulls_avant > 0:
            df[colonnes_texte] = df[colonnes_texte].fillna(valeur_defaut)
            logger.info(f"  ✓ {nb_nulls_avant} valeurs NULL remplacées")
            self.metadata['transformations_appliquees'].append('remplissage_valeurs_manquantes')
        else:
            logger.info("  ✓ Aucune valeur NULL dans colonnes texte")
        
        return df
    
    def _detecter_colonnes_sensibles(self, df: pd.DataFrame):
        """
        Détecte les colonnes potentiellement sensibles.
        SÉCURITÉ : Émet des warnings si données sensibles non anonymisées
        """
        patterns = self.config.get('colonnes_sensibles_patterns', [])
        colonnes_sensibles_detectees = []
        
        for col in df.columns:
            for pattern in patterns:
                if pattern.lower() in col.lower():
                    colonnes_sensibles_detectees.append(col)
                    logger.warning(
                        f"🔒 ATTENTION : Colonne sensible détectée '{col}' - "
                        f"Vérifiez l'anonymisation !"
                    )
                    break
        
        if colonnes_sensibles_detectees:
            self.metadata['warnings'].append({
                'type': 'DONNEES_SENSIBLES',
                'colonnes': colonnes_sensibles_detectees,
                'message': 'Colonnes potentiellement sensibles sans anonymisation explicite'
            })
