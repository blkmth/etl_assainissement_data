"""
Orchestrateur principal des transformations.
Décide d'appliquer transformation spécifique ou par défaut.
"""
import pandas as pd
import yaml
from typing import Dict, Any, Tuple, Optional
from pathlib import Path
from src.transform.transformateur_tpar_defaut import TransformateurParDefaut
from src.transform.transformation_specifique import REGISTRE_TRANSFORMATIONS
from src.config.logging_config import setup_logger

logger = setup_logger(__name__)


def charger_configuration(chemin_config: str = "src/transform/config.yaml") -> Dict[str, Any]:
    """
    Charge la configuration depuis le fichier YAML.
    
    Args:
        chemin_config: Chemin vers config.yaml
    
    Returns:
        Dictionnaire de configuration
    """
    chemin = Path(chemin_config)
    
    if not chemin.exists():
        raise FileNotFoundError(f"❌ Fichier config non trouvé : {chemin_config}")
    
    with open(chemin, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"✅ Configuration chargée depuis {chemin_config}")
    return config


def resoudre_alias_table(nom_table: str, config: Dict[str, Any]) -> str:
    """
    Résout les alias de tables (ex: 'finances' → 'clients').
    
    Args:
        nom_table: Nom de table donné
        config: Configuration complète
    
    Returns:
        Nom canonical de la table
    """
    tables_spec = config.get('tables_specifiques', {})
    
    for nom_canonical, config_table in tables_spec.items():
        alias = config_table.get('alias', [])
        
        if nom_table.lower() in [a.lower() for a in alias]:
            logger.info(f"🔄 Alias résolu : '{nom_table}' → '{nom_canonical}'")
            return nom_canonical
        
        if nom_table.lower() == nom_canonical.lower():
            return nom_canonical
    
    return nom_table


def transformer_table(
    df: pd.DataFrame,
    nom_table: str,
    config: Optional[Dict[str, Any]] = None,
    chemin_config: str = "src/transform/config.yaml"
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    🎯 FONCTION PRINCIPALE : Orchestre la transformation d'une table.
    
    LOGIQUE :
    1. Charge la configuration si non fournie
    2. Résout les alias de tables
    3. Vérifie si transformation spécifique existe
    4. Applique transformation spécifique OU par défaut
    5. Applique règles supplémentaires de la config
    6. Retourne (DataFrame transformé, métadonnées complètes)
    
    Args:
        df: DataFrame à transformer
        nom_table: Nom de la table
        config: Configuration (optionnel, sinon chargé depuis YAML)
        chemin_config: Chemin vers config.yaml
    
    Returns:
        Tuple (DataFrame transformé, métadonnées enrichies)
    
    Exemple:
        >>> df_clients = pd.read_csv('clients.csv')
        >>> df_clean, meta = transformer_table(df_clients, 'clients')
        >>> print(meta['transformations_appliquees'])
    """
    logger.info("")
    logger.info("="*80)
    logger.info(f"🚀 DÉBUT TRANSFORMATION TABLE : {nom_table}")
    logger.info("="*80)
    
    # Étape 1 : Charger configuration
    if config is None:
        config = charger_configuration(chemin_config)
    
    # Étape 2 : Résoudre alias
    nom_canonical = resoudre_alias_table(nom_table, config)
    
    # Étape 3 : Déterminer type de transformation
    fonction_specifique = REGISTRE_TRANSFORMATIONS.get(nom_canonical.lower())
    
    metadata_complete = {
        'nom_table': nom_table,
        'nom_canonical': nom_canonical,
        'type_transformation': None,
        'metadata_defaut': {},
        'metadata_specifique': {}
    }
    
    if fonction_specifique:
        # === TRANSFORMATION SPÉCIFIQUE ===
        logger.info(f"✨ Transformation SPÉCIFIQUE détectée pour '{nom_canonical}'")
        metadata_complete['type_transformation'] = 'specifique'
        
        # Appliquer d'abord le transformateur par défaut
        config_default = config.get('regles_default', {})
        transformateur_defaut = TransformateurParDefaut(config_default)
        df_defaut, meta_defaut = transformateur_defaut.transformer(df, nom_canonical)
        metadata_complete['metadata_defaut'] = meta_defaut
        
        # Puis appliquer la transformation spécifique
        config_table = config.get('tables_specifiques', {}).get(nom_canonical, {})
        df_final, meta_specifique = fonction_specifique(df_defaut, config_table)
        metadata_complete['metadata_specifique'] = meta_specifique
    
    else:
        # === TRANSFORMATION PAR DÉFAUT UNIQUEMENT ===
        logger.info(f"🔧 Aucune transformation spécifique trouvée pour '{nom_canonical}'")
        logger.info("→ Application des transformations PAR DÉFAUT")
        metadata_complete['type_transformation'] = 'defaut_uniquement'
        
        config_default = config.get('regles_default', {})
        transformateur_defaut = TransformateurParDefaut(config_default)
        df_final, meta_defaut = transformateur_defaut.transformer(df, nom_canonical)
        metadata_complete['metadata_defaut'] = meta_defaut
    
    # Étape 4 : Génération des métadonnées finales
    metadata_complete['nombre_lignes_final'] = len(df_final)
    metadata_complete['nombre_colonnes_final'] = len(df_final.columns)
    metadata_complete['colonnes_finales'] = df_final.columns.tolist()
    
    # Statistiques de qualité
    metadata_complete['qualite'] = {
        'taux_completude': (1 - df_final.isnull().sum().sum() / 
                           (len(df_final) * len(df_final.columns))) * 100,
        'colonnes_avec_nulls': df_final.columns[df_final.isnull().any()].tolist(),
        'doublons_restants': df_final.duplicated().sum()
    }
    
    logger.info("="*80)
    logger.info(f"✅ TRANSFORMATION TERMINÉE : {nom_canonical}")
    logger.info(f"📊 Lignes : {len(df)} → {len(df_final)}")
    logger.info(f"📊 Colonnes : {len(df.columns)} → {len(df_final.columns)}")
    logger.info(f"📊 Qualité : {metadata_complete['qualite']['taux_completude']:.1f}% complétude")
    logger.info("="*80)
    logger.info("")
    
    return df_final, metadata_complete
