--creation de la base de données
CREATE DATABASE IF NOT EXISTS données_propres
character set utf8mb4 COLLATE utf8mb4_unicode_ci;

USE données_propres;

--creation des tables : TABLES FINACES NETTOYÉS
CREATE TABLE IF NOT EXISTS finances_propres (
    id INT PRIMARY KEY AUTO_INCREMENT,
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    pays VARCHAR(100) NOT NULL,
    revenu_annuel DECIMAL(15,2),
    depenses_annuelles DECIMAL(15,2),
    solde_compte DECIMAL(15,2),
    numero_carte VARCHAR(20),
    transaction_mensuelle INT,
    taux_epargne DECIMAL(5,2), -- Colonne calculée
    statut_financier VARCHAR(50), -- Colonne enrichie
    date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_pays (pays),
    INDEX idx_email (email)
) ENGINE=InnoDB

--creation des tables : TABLES ventes NETTOYÉS 
CREATE TABLE IF NOT EXISTS ventes_propres (
    id INT PRIMARY KEY AUTO_INCREMENT,
    year INT NOT NULL,
    make VARCHAR(100) NOT NULL,
    model VARCHAR(100) NOT NULL,
    trim VARCHAR(100),
    body VARCHAR(50),
    transmission VARCHAR(50),
    vin VARCHAR(17) UNIQUE,
    state VARCHAR(2),
    condition_vehicule VARCHAR(50),
    odometer INT,
    color VARCHAR(50),
    interior VARCHAR(50),
    seller VARCHAR(255),
    mmr DECIMAL(10,2), -- Market Mean Retail
    sellingprice DECIMAL(10,2),
    saledate DATE,
    marge_beneficiaire DECIMAL(10,2), -- Colonne calculée
    annee_modele_valide BOOLEAN,
    date_insertion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_make_model (make, model),
    INDEX idx_year (year),
    INDEX idx_saledate (saledate)
) ENGINE=InnoDB;

-- Table de métriques qualité
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id INT PRIMARY KEY AUTO_INCREMENT,
    table_name VARCHAR(100),
    execution_date TIMESTAMP,
    total_records INT,
    valid_records INT,
    invalid_records INT,
    duplicate_records INT,
    null_percentage DECIMAL(5,2),
    quality_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;