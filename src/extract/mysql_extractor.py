"""
Module d'extraction depuis MySQL.
RESPONSABILITÉ: Extraire les données brutes des tables sources
"""
import pandas as pd
from typing import Dict, Any, List
from sqlalchemy import inspect

# Import de la configuration de connexion à la base et du logger centralisé.
# `DatabaseConfig` encapsule la création d'un moteur SQLAlchemy configuré
# (URL, pool, options). Le module de logging fournit un logger cohérent
# pour que les messages d'extraction suivent la même configuration de log
# que le reste du projet.
from src.config.database import DatabaseConfig
from src.config.logging_config import setup_logger

logger = setup_logger(__name__)

class MySQLExtractor:
    """Classe d'extraction pour MySQL"""
    
    def __init__(self):
        # Le moteur SQLAlchemy est utilisé par pandas.read_sql et par
        # sqlalchemy.inspect pour interroger le schéma et exécuter des requêtes.
        # Il doit déjà être configuré (host/port/user/password/db) dans
        # `DatabaseConfig`.
        self.engine = DatabaseConfig.get_mysql_source_engine()

    def list_tables(self) -> List[str]:
        """
        Récupère la liste des tables présentes dans la base MySQL connectée.

        Returns:
            Liste de noms de tables (list[str])
        """
        # Utilise l'inspecteur SQLAlchemy pour récupérer la liste des
        # tables visibles pour la connexion (schéma/database courant).
        # Ceci évite d'avoir à connaître manuellement les noms de tables.
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            logger.info(f"Tables trouvées: {tables}")
            return tables

        except Exception as e:
            # En cas d'erreur (problème de connexion, permissions, etc.), on
            # logge l'erreur puis on retransmet l'exception pour que l'appelant
            # puisse décider de la stratégie (retry, abort, fallback).
            logger.error(f"Erreur récupération liste de tables: {e}")
            raise

    def extract_table(self, table_name: str, limit: int = None) -> pd.DataFrame:
        """
        Extraction générique d'une table MySQL (SELECT *).

        Args:
            table_name: nom de la table à extraire
            limit: optionnel, limite le nombre de lignes retournées

        Returns:
            DataFrame avec les données de la table
        """
        # Construction d'une requête simple SELECT * pour extraire la table.
        # On entoure `table_name` de backticks pour gérer les noms contenant
        # des caractères spéciaux ou des mots réservés. ATTENTION: si
        # `table_name` provient d'une source non fiable, il faudrait appliquer
        # une validation stricte pour éviter l'injection SQL.
        query = f"SELECT * FROM `{table_name}`"

        # Si l'appelant a demandé une limite, on l'ajoute à la requête. On
        # convertit en int pour éviter les injections via des chaînes malicieuses.
        if limit is not None:
            try:
                limit_val = int(limit)
                query += f" LIMIT {limit_val}"
            except Exception:
                # Si la conversion échoue, on ignore la limite et on continue
                # l'extraction complète tout en prévenant dans les logs.
                logger.warning("Paramètre limit invalide: on l'ignore")

        # Exécution de la requête via pandas qui utilise SQLAlchemy sous le capot.
        # pandas.read_sql renvoie un DataFrame et gère la conversion des types.
        try:
            df = pd.read_sql(query, self.engine)
            logger.info(f"✓ Extraction réussie {table_name}: {len(df)} lignes")
            return df

        except Exception as e:
            # Toute erreur est loggée et retransmise: utile pour superviser
            # les problèmes (permissions, timeout, schéma inattendu...).
            logger.error(f"✗ Erreur extraction table {table_name}: {e}")
            raise

    def extract_all_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Extrait toutes les tables présentes dans la base et retourne un dictionnaire
        mapping table_name -> DataFrame. En cas d'erreur sur une table, elle est
        sautée avec log.
        """
        # Récupère toutes les tables et tente d'extraire chacune.
        # Les tables qui posent problème sont sautées pour que l'extraction
        # globale reste résiliente (pratique pour exploration / debug).
        tables = self.list_tables()
        results: Dict[str, pd.DataFrame] = {}
        for t in tables:
            try:
                results[t] = self.extract_table(t)
            except Exception as e:
                # On ne lève pas l'exception ici pour permettre l'extraction des
                # autres tables; on logge en warning pour informer l'opérateur.
                logger.warning(f"Échec extraction table {t}: {e} (sautée)")
        return results
    
    def get_extraction_metadata(self, table_name: str) -> Dict[str, Any]:
        """
        Récupère des métadonnées sur la table source.
        UTILITÉ: Monitoring et validation
        
        Returns:
            Dictionnaire avec count, min_id, max_id, etc.
        """
        # On récupère d'abord le nombre total de lignes: opération simple et
        # généralement supportée par toutes les tables/permissions.
        metadata: Dict[str, Any] = {}
        count_q = f"SELECT COUNT(*) AS total_rows FROM `{table_name}`"
        try:
            result = pd.read_sql(count_q, self.engine)
            metadata['total_rows'] = int(result['total_rows'].iloc[0])
        except Exception as e:
            # Si même le COUNT échoue (problème de permissions / table inexistante),
            # on retourne un dictionnaire vide pour indiquer l'échec.
            logger.warning(f"Impossible de récupérer COUNT(*) pour {table_name}: {e}")
            return {}

        # Ensuite, si la table possède une colonne `id` numérique, on essaye de
        # récupérer min/max. Cette opération peut échouer pour des tables sans
        # `id` ou si la colonne n'est pas comparable; donc on capture et on
        # ignore l'erreur tout en la notant en debug.
        try:
            id_q = f"SELECT MIN(id) as min_id, MAX(id) as max_id FROM `{table_name}`"
            r = pd.read_sql(id_q, self.engine)
            if r is not None and not r.empty:
                metadata['min_id'] = r.loc[0, 'min_id']
                metadata['max_id'] = r.loc[0, 'max_id']
        except Exception:
            # Pas critique: si `id` n'existe pas, on continue sans min/max
            logger.debug(f"Colonne 'id' non présente ou inaccessible pour {table_name}")

        logger.info(f"Métadonnées {table_name}: {metadata}")
        return metadata
