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
    

# ============================================================================
# FONCTION 2: TRANSFORMATEUR SPÉCIALISÉ POUR LA TABLE "CLIENTS"
# ============================================================================
    """
    Expert spécialisé pour la table clients.
    
    Responsabilités spécifiques:
        - Anonymiser les numéros de carte bancaire
        - Calculer des indicateurs métier (taux d'endettement, solde net)
        - Normaliser les codes pays
        - Valider la présence de colonnes obligatoires
        
    Héritage:
        Réutilise TOUT le traitement standard du TransformateurParDefaut
        puis ajoute les traitements spécialisés par-dessus.
    
    Args:
        df: DataFrame de la table clients
        config: Configuration YAML
        
    Returns:
        Tuple (DataFrame traité, métadonnées enrichies)
    """
## afficher un avertissement de dépréciation
print ("DONNÉES SENSIBLES DETECTÉES :TION DU RTAITEMENT SPÉCIALISÉ CLIENTS EST DÉPRÉCIÉE")

# ---------------------------------------------------------------
# ETAPE 1: APPELER LE TRANSFORMATEUR PAR DÉFAUT (LE TRAITEMENT STANDARD)
# ---------------------------------------------------------------

# Principe de réutilisation: on hérite de tous les nettoyages de base
    # (normalisation colonnes, dates, doublons, valeurs manquantes)

TransformateurParDefaut = TransformateurParDefaut(config)
df_clean, metadata = TransformateurParDefaut.transformer(df, "clients")

## mettre à jour le type de traitement dans les métadonnées
metadata["traitement"] = "SPÉCIALISÉ_CLIENTS"

# ---------------------------------------------------------------
# ETAPE 2: anonymisation DES NUMÉROS DE CARTE BANCAIRE
# ---------------------------------------------------------------
    # Objectif: Masquer les données sensibles (RGPD, PCI-DSS)
    # Stratégie: Garder seulement les 4 derniers chiffres
    
if 'numero_carte' in df_clean.columns : 
    ## enregistrer le nombre de cartes avant anonymisation
    metadata["cartes_avant_anonymisation"] = len(df_clean)

    ## appliquer l'anonymisation
    def masquer_carte(numero):
        """
     Masque un numéro de carte en gardant les 4 derniers chiffres.
            
            Exemples:
                "1234567890123456" → "************3456"
                "123" → "***"
                NaN → "INCONNU"
        """

        ## cas 1: valeur manquante ou deja remplacée
        if pd.isna(numero) or numero == "INCONNU":
            return "INCONNU"
        
        ## cas 2: convertir en string pour manipulation
        numero_str = str(numero)   

        ## cas 3: masquer tous les chiffres sauf les 4 derniers
        if len(numero_str) <= 4:
                return "*" * len(numero_str)
        
        ## Sinon: masquer tout sauf les 4 derniers
                return "*" * (len(numero_str) - 4) + numero_str[-4:]
        
    ## appliquer la fonction d'anonymisation à toute la colonne
    df_clean['numero_carte'] = df_clean['numero_carte'].apply(masquer_carte)

    ## documenter dansles metadonnées
    metadata["action_anonymisation"] = "Numéros de carte bancaire anonymisés"
    
    # ----------------------------------------------------------------
    # ÉTAPE 3: CALCULS MÉTIER AUTOMATIQUES
    # ----------------------------------------------------------------
    
    ## Objectif: Créer de nouvelles colonnes calculées pour l'analyse

    # Récupérer la liste des calculs à effectuer depuis la config
    calculs = config.get("tables_specialisées", {}).get("clients", {}).get("calculs_metier", {})

    try:
        #--------- calcul 1 : taux d'endettement -------------------------
        if "taux d'endettement" in calculs:
            df_clean['taux_endettement'] = (
                df_clean['depenses_annuelles'].astype(float) /
                df_clean['revenu_annuel'].astype(float
            ). round(2) * 100  # en pourcentage arrondi à 2 décimales

            ## initialiser la liste des calculs crées
            metadata["calculs_crees"] = ["taux endetetement"]

        #--------- calcul 2 : solde net -------------------------
        if "solde_net" in calculs:
            df_clean['solde_net'] = (
                df_clean['revenu_annuel'].astype(float) -
                df_clean['depenses_annuelles'].astype(float)
            ) round(2) # arrondi à 2 décimales

            ## documenter dans les métadonnées
            metadata["calculs_crees"].append("solde_net")

    except KeyError as e:
        # En cas d'erreur (colonne manquante, type incorrect, etc.)
        # Ne pas crasher, juste enregistrer l'erreur
        metadata["erreurs_calculs"] = str(e)

 ])
    
    # ----------------------------------------------------------------
    # ÉTAPE 5: VALIDATION DES COLONNES OBLIGATOIRES
    # ----------------------------------------------------------------
    # Objectif: Vérifier que les colonnes critiques sont présentes
    # Exemple: un client DOIT avoir nom, prénom, email
    
    # Récupérer la liste des colonnes obligatoires depuis la config
    obligatoires = config.get("tables_specifiques", {}).get("clients", {}).get("colonnes_obligatoires", [])
    
    # Identifier quelles colonnes obligatoires sont manquantes
    colonnes_manquantes = [col for col in obligatoires if col not in df_clean.columns]
    
    # Si des colonnes manquent, émettre un warning
    if colonnes_manquantes:
        # Ajouter une alerte dans les métadonnées
        metadata["alertes"] = f"Colonnes obligatoires manquantes: {colonnes_manquantes}"
        
        # Émettre un warning Python (visible dans les logs)
        warnings.warn(f"⚠️ Table clients - Colonnes manquantes: {colonnes_manquantes}")
    
    # ----------------------------------------------------------------
    # RETOUR: DataFrame enrichi + métadonnées détaillées
    # ----------------------------------------------------------------
    return df_clean, metadata