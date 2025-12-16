"""
configuration des connexions aux bases de données.
gerer les connexion a Mysql et Postgresql via SQLAlchemy.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()  # Charger les variables d'environnement depuis le fichier .env

class DatabaseConfig:
    """
    config centralisée des connexions aux DB
    """

    ## mysql source
    @staticmethod
    def get_mysql_source_engine():
        """
        retourne l'engine de connexion mysql
        pour centraliser la connexion et eviter la duplication de code.
        """

        connection_string = (
            ## mysql source
            f"mysql+pymysql://{os.getenv('MYSQL_USER')}:"
            ## password
            f"{os.getenv('MYSQL_PASSWORD')}@"
            ## host et port
            f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/"
            ## database
            f"{os.getenv('MYSQL_DB_SOURCE')}"
        )

        return create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        echo=False
    )

    @staticmethod
    def get_mysql_target_engine():
        """
        engine pour la base cible mysql
        """
        connexion_string = (
            ## mysql target
            f"mysql+pymysql://{os.getenv('MYSQL_USER')}:"
            ## password 
            f"{os.getenv('MYSQL_PASSWORD')}@"
            ## host et port
            f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/"
            ## database
            f"{os.getenv('MYSQL_DB_TARGET')}"
        )
        
        return create_engine(
    connexion_string,
    pool_pre_ping=True,
)
    
    @staticmethod
    def get_postgresql_source_engine():
        """
        retourne l'engine de connexion postgresql source
        """
        connexion_string = (
            ## postgresql source
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
            ## password
            f"{os.getenv('POSTGRES_PASSWORD')}@"
            ## host et port
            f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
            ## database
            f"{os.getenv('POSTGRES_DB_SOURCE')}"
        )
        return create_engine(
            connexion_string,
            pool_pre_ping=True,
            )
    
    @staticmethod
    def get_postgresql_target_engine():
        """
        retourne l'engine de connexion postgresql cible
        """
        connexion_string = (
            ## postgresql target
            f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
            ## password
            f"{os.getenv('POSTGRES_PASSWORD')}@"
            ## host et port
            f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
            ## database
            f"{os.getenv('POSTGRES_DB_TARGET')}"
        )
        return create_engine(
            connexion_string,
            pool_pre_ping=True,
            )