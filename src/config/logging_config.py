"""
configuration centralisée pour le logging de l'application.
pour des log plus structurés pour un meilleur debugging
"""

import os
import logging
from datetime import datetime

def setup_logger(name:str) -> logging.Logger:
    """"
    configure et retourne un logger avec un fichier et console en sortie

    Args:
        name (str): nom du logger (généralement __name__ du module)
    
    Returns:
        logger configuré
    """

    ## creer le dossier log s"il n'existe pas
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    ## nom du fichier de log avec timestamp
    log_file = os.path.join (
        log_dir,
        f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    ## configurer le logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    ## Eviter d'ajouter plusieurs handlers si le logger est déjà configuré
    if not logger.hasHandlers:
        ##handler pour le fichier de log
        file_handler = logging.FileHandler(log_file , encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        ## handler pour la console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        ## format des logs
        formatter = logging.Formatter (
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s' ,
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        ## ajouter les formatters aux handlers
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter) 

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)  

    return logger