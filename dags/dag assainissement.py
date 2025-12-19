"""
DAG Airflow pour pipeline ETL d'assainissement de données.

ARCHITECTURE: Séquentielle (pas de parallélisation)
FRÉQUENCE: Quotidienne à 2h du matin
RETRY: 3 tentatives avec délai de 5 minutes

FLUX:
start → create_schemas → extract_finances → transform_finances → load_finances
                      → extract_ventes → transform_ventes → load_ventes → end
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Import des modules ETL
import sys
from pathlib import Path

# Ajouter le répertoire racine du projet au PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extract.mysql_extractor import MySQLExtractor
from src.transform.orchestrateur import transformer_table
from src.load.mysql_loader import MySQLLoader
from src.config.logging_config import setup_logger

logger = setup_logger(__name__)

# Configuration par défaut du DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,  # Ne pas dépendre de l'exécution précédente
    'start_date': datetime(2025, 12, 1),
    'email': ['data@votreentreprise.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,  # 3 tentatives en cas d'échec
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)  # Timeout à 2h
}

# Définition du DAG
dag = DAG(
    'etl_assainissement_donnees',
    default_args=default_args,
    description='Pipeline ETL pour assainissement données financières et ventes',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,  # Ne pas exécuter les runs manqués
    max_active_runs=1,  # Une seule exécution à la fois
    tags=['etl', 'assainissement', 'production']
)

# ============= FONCTIONS PYTHON POUR CHAQUE TÂCHE =============

def create_database_schemas(**context):
    """Tâche 1: Créer les schémas de base de données cibles"""
    logger.info("="*50)
    logger.info("TÂCHE: Création des schémas de base de données")
    logger.info("="*50)
    
    loader = MySQLLoader()
    loader.create_tables_if_not_exist()
    
    logger.info("✓ Schémas créés avec succès")

def extract_finances_task(**context):
    """Tâche 2: Extraire les données financières"""
    logger.info("="*50)
    logger.info("TÂCHE: Extraction données FINANCES")
    logger.info("="*50)
    
    extractor = MySQLExtractor()
    # Utiliser extract_table avec le nom de la table source
    # Note: Ajustez le nom de table selon votre schéma source
    df = extractor.extract_table('finances')  # ou le nom réel de votre table source
    
    # Sauvegarder dans XCom pour passer aux tâches suivantes
    # Note: Pour gros volumes, utiliser stockage externe (S3, etc.)
    context['task_instance'].xcom_push(
        key='finances_raw', 
        value=df.to_json(orient='records')
    )
    
    logger.info(f"✓ {len(df)} lignes extraites et stockées dans XCom")

def transform_finances_task(**context):
    """Tâche 3: Transformer les données financières"""
    logger.info("="*50)
    logger.info("TÂCHE: Transformation données FINANCES")
    logger.info("="*50)
    
    import pandas as pd
    
    # Récupérer données depuis XCom
    finances_json = context['task_instance'].xcom_pull(
        task_ids='extract_finances',
        key='finances_raw'
    )
    df = pd.read_json(finances_json, orient='records')
    
    # Appliquer transformation avec le nouvel orchestrateur
    df_clean, metadata = transformer_table(
        df,
        'clients',  # ou 'finances' selon votre config
        chemin_config='src/transform/config.yaml'
    )
    
    # Extraire les statistiques depuis les métadonnées
    stats = {
        'total_rows': metadata.get('nombre_lignes_final', len(df_clean)),
        'duplicates_removed': metadata.get('metadata_defaut', {}).get('lignes_supprimees_doublons', 0),
        'invalid_emails': 0,  # À adapter selon vos validations
        'invalid_years': 0,
        'quality_score': metadata.get('qualite', {}).get('taux_completude', 0)
    }
    
    # Stocker résultat et métriques
    context['task_instance'].xcom_push(
        key='finances_clean',
        value=df_clean.to_json(orient='records')
    )
    context['task_instance'].xcom_push(
        key='finances_stats',
        value=stats
    )
    
    logger.info(f"✓ Transformation terminée: {len(df_clean)} lignes valides")
    logger.info(f"📊 Statistiques: {stats}")

def load_finances_task(**context):
    """Tâche 4: Charger les données financières"""
    logger.info("="*50)
    logger.info("TÂCHE: Chargement données FINANCES")
    logger.info("="*50)
    
    import pandas as pd
    
    # Récupérer données nettoyées
    finances_json = context['task_instance'].xcom_pull(
        task_ids='transform_finances',
        key='finances_clean'
    )
    df = pd.read_json(finances_json, orient='records')
    
    # Charger dans base cible
    loader = MySQLLoader()
    rows_loaded = loader.load_finances(df)
    
    # Enregistrer métriques
    stats = context['task_instance'].xcom_pull(
        task_ids='transform_finances',
        key='finances_stats'
    )
    loader.load_quality_metrics(
        stats, 
        'finances', 
        context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
    )
    
    logger.info(f"✓ {rows_loaded} lignes chargées avec succès")

def extract_ventes_task(**context):
    """Tâche 5: Extraire les données de ventes"""
    logger.info("="*50)
    logger.info("TÂCHE: Extraction données VENTES")
    logger.info("="*50)
    
    extractor = MySQLExtractor()
    # Utiliser extract_table avec le nom de la table source
    # Note: Ajustez le nom de table selon votre schéma source
    df = extractor.extract_table('ventes')  # ou le nom réel de votre table source
    
    context['task_instance'].xcom_push(
        key='ventes_raw',
        value=df.to_json(orient='records')
    )
    
    logger.info(f"✓ {len(df)} lignes extraites")

def transform_ventes_task(**context):
    """Tâche 6: Transformer les données de ventes"""
    logger.info("="*50)
    logger.info("TÂCHE: Transformation données VENTES")
    logger.info("="*50)
    
    import pandas as pd
    
    ventes_json = context['task_instance'].xcom_pull(
        task_ids='extract_ventes',
        key='ventes_raw'
    )
    df = pd.read_json(ventes_json, orient='records')
    
    # Appliquer transformation avec le nouvel orchestrateur
    df_clean, metadata = transformer_table(
        df,
        'vehicules',  # ou 'ventes' selon votre config
        chemin_config='src/transform/config.yaml'
    )
    
    # Extraire les statistiques depuis les métadonnées
    stats = {
        'total_rows': metadata.get('nombre_lignes_final', len(df_clean)),
        'duplicates_removed': metadata.get('metadata_defaut', {}).get('lignes_supprimees_doublons', 0),
        'invalid_emails': 0,
        'invalid_years': 0,
        'quality_score': metadata.get('qualite', {}).get('taux_completude', 0)
    }
    
    context['task_instance'].xcom_push(
        key='ventes_clean',
        value=df_clean.to_json(orient='records')
    )
    context['task_instance'].xcom_push(
        key='ventes_stats',
        value=stats
    )
    
    logger.info(f"✓ Transformation terminée: {len(df_clean)} lignes valides")

def load_ventes_task(**context):
    """Tâche 7: Charger les données de ventes"""
    logger.info("="*50)
    logger.info("TÂCHE: Chargement données VENTES")
    logger.info("="*50)
    
    import pandas as pd
    
    ventes_json = context['task_instance'].xcom_pull(
        task_ids='transform_ventes',
        key='ventes_clean'
    )
    df = pd.read_json(ventes_json, orient='records')
    
    loader = MySQLLoader()
    rows_loaded = loader.load_ventes(df)
    
    stats = context['task_instance'].xcom_pull(
        task_ids='transform_ventes',
        key='ventes_stats'
    )
    loader.load_quality_metrics(
        stats,
        'ventes',
        context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
    )
    
    logger.info(f"✓ {rows_loaded} lignes chargées avec succès")

# ============= DÉFINITION DES TÂCHES AIRFLOW =============

start = EmptyOperator(task_id='start', dag=dag)

create_schemas = PythonOperator(
    task_id='create_schemas',
    python_callable=create_database_schemas,
    dag=dag
)

extract_finances = PythonOperator(
    task_id='extract_finances',
    python_callable=extract_finances_task,
    dag=dag
)

transform_finances = PythonOperator(
    task_id='transform_finances',
    python_callable=transform_finances_task,
    dag=dag
)

load_finances = PythonOperator(
    task_id='load_finances',
    python_callable=load_finances_task,
    dag=dag
)

extract_ventes = PythonOperator(
    task_id='extract_ventes',
    python_callable=extract_ventes_task,
    dag=dag
)

transform_ventes = PythonOperator(
    task_id='transform_ventes',
    python_callable=transform_ventes_task,
    dag=dag
)

load_ventes = PythonOperator(
    task_id='load_ventes',
    python_callable=load_ventes_task,
    dag=dag
)

end = EmptyOperator(task_id='end', dag=dag)

# ============= DÉFINITION DU FLUX SÉQUENTIEL =============

start >> create_schemas

# Branche finances
create_schemas >> extract_finances >> transform_finances >> load_finances

# Branche ventes (après finances)
load_finances >> extract_ventes >> transform_ventes >> load_ventes

load_ventes >> end
