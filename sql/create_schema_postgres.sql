CREATE DATABASE données_saines 
WITH ENCODING='UTF8' 
LC_COLLATE='fr_FR.UTF-8' 
LC_CTYPE='fr_FR.UTF-8';

-- Schéma pour les données nettoyées
CREATE SCHEMA IF NOT EXISTS clean_data;

-- Table finances PostgreSQL (si source)
CREATE TABLE IF NOT EXISTS clean_data.finances_clean (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    pays VARCHAR(100) NOT NULL,
    revenu_annuel NUMERIC(15,2),
    depenses_annuelles NUMERIC(15,2),
    solde_compte NUMERIC(15,2),
    numero_carte VARCHAR(20),
    transaction_mensuelle INTEGER,
    taux_epargne NUMERIC(5,2),
    statut_financier VARCHAR(50),
    date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_finances_pays ON clean_data.finances_clean(pays);
CREATE INDEX idx_finances_email ON clean_data.finances_clean(email);
