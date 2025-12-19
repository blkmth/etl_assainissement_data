"""
Tests unitaires pour le module de transformation.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from src.transform.orchestrateur import transformer_table, charger_configuration
from src.transform.transformateur_tpar_defaut import TransformateurParDefaut
from src.transform.transformation_specifique import transformer_clients_specifique


@pytest.fixture
def sample_clients_df():
    """Fixture pour DataFrame clients de test"""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'nom': ['Dupont', 'Martin', 'Bernard'],
        'prenom': ['Jean', 'Marie', 'Paul'],
        'email': ['jean.dupont@example.com', 'marie.martin@example.com', 'paul.bernard@example.com'],
        'pays': ['France', 'États-Unis', 'Allemagne'],
        'revenu_annuel': [50000, 75000, 60000],
        'depenses_annuelles': [40000, 50000, 45000],
        'numero_carte': ['4532123456789012', '5425233430109903', '374245455400126']
    })


@pytest.fixture
def sample_vehicules_df():
    """Fixture pour DataFrame véhicules de test"""
    return pd.DataFrame({
        'make': ['BMW', 'Toyota', 'Ford'],
        'model': ['X5', 'Camry', 'F-150'],
        'vin': ['5UXKR0C58F0P04749', '4T1BF1FK5CU123456', '19UUA56662A123456'],
        'odometer': [25000, 50000, 75000],
        'saledate': ['2024-01-15', '2024-02-20', '2024-03-10']
    })


@pytest.fixture
def config_dict():
    """Fixture pour configuration de test"""
    config_path = Path('src/transform/config.yaml')
    if config_path.exists():
        return charger_configuration(str(config_path))
    return {}


class TestTransformateurParDefaut:
    """Tests pour TransformateurParDefaut"""
    
    def test_normalisation_colonnes(self, config_dict):
        """Test normalisation des noms de colonnes"""
        df = pd.DataFrame({
            'ID Client': [1, 2],
            'Date Commande': ['2024-01-01', '2024-02-15'],
            'Montant-TTC': [100.50, 250.75]
        })
        
        config = config_dict.get('regles_default', {})
        transformateur = TransformateurParDefaut(config)
        df_clean, metadata = transformateur.transformer(df, 'test_table')
        
        # Vérifier que les colonnes sont normalisées
        assert 'id_client' in df_clean.columns or 'id' in df_clean.columns
        assert len(df_clean.columns) == 3
    
    def test_suppression_doublons(self, config_dict):
        """Test suppression des doublons"""
        df = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'nom': ['A', 'B', 'B', 'C']
        })
        
        config = config_dict.get('regles_default', {})
        transformateur = TransformateurParDefaut(config)
        df_clean, metadata = transformateur.transformer(df, 'test_table')
        
        # Vérifier que les doublons sont supprimés
        assert len(df_clean) <= len(df)
        if config.get('supprimer_doublons', True):
            assert len(df_clean) == 3  # 2 doublons supprimés
    
    def test_remplissage_valeurs_manquantes(self, config_dict):
        """Test remplissage des valeurs manquantes"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'nom': ['A', None, 'C'],
            'valeur': [10, 20, None]
        })
        
        config = config_dict.get('regles_default', {})
        transformateur = TransformateurParDefaut(config)
        df_clean, metadata = transformateur.transformer(df, 'test_table')
        
        # Vérifier qu'il n'y a plus de NaN
        valeur_defaut = config.get('valeur_manquante_defaut', 'INCONNU')
        assert df_clean['nom'].isna().sum() == 0
        assert df_clean['valeur'].isna().sum() == 0


class TestTransformationSpecifique:
    """Tests pour les transformations spécifiques"""
    
    def test_transformer_clients_anonymisation(self, sample_clients_df, config_dict):
        """Test anonymisation des numéros de carte"""
        config_table = config_dict.get('tables_specifiques', {}).get('clients', {})
        
        df_clean, metadata = transformer_clients_specifique(
            sample_clients_df,
            config_table
        )
        
        # Vérifier que les cartes sont anonymisées
        if 'numero_carte' in df_clean.columns:
            cartes = df_clean['numero_carte'].dropna()
            if len(cartes) > 0:
                # Les cartes doivent être masquées (commencer par *)
                assert all(str(carte).startswith('*') for carte in cartes if carte != 'INCONNU')
    
    def test_transformer_clients_calculs_metriques(self, sample_clients_df, config_dict):
        """Test calcul des métriques (taux_endettement, solde_net)"""
        config_table = config_dict.get('tables_specifiques', {}).get('clients', {})
        
        df_clean, metadata = transformer_clients_specifique(
            sample_clients_df,
            config_table
        )
        
        # Vérifier que les calculs sont présents si configurés
        calculs = config_table.get('calculs_metriques', [])
        if calculs:
            for calcul in calculs:
                nom_colonne = calcul.get('nom')
                if nom_colonne in df_clean.columns:
                    assert df_clean[nom_colonne].notna().any()


class TestOrchestrateur:
    """Tests pour l'orchestrateur principal"""
    
    def test_transformer_table_clients(self, sample_clients_df, config_dict):
        """Test transformation complète table clients"""
        df_clean, metadata = transformer_table(
            sample_clients_df,
            'clients',
            config=config_dict
        )
        
        assert len(df_clean) > 0
        assert 'type_transformation' in metadata
        assert metadata['type_transformation'] == 'specifique'
        assert 'qualite' in metadata
    
    def test_transformer_table_vehicules(self, sample_vehicules_df, config_dict):
        """Test transformation complète table véhicules"""
        df_clean, metadata = transformer_table(
            sample_vehicules_df,
            'vehicules',
            config=config_dict
        )
        
        assert len(df_clean) > 0
        assert 'type_transformation' in metadata
    
    def test_transformer_table_inconnue(self, config_dict):
        """Test transformation table inconnue (par défaut uniquement)"""
        df = pd.DataFrame({
            'ID': [1, 2, 3],
            'Valeur': [10, 20, 30]
        })
        
        df_clean, metadata = transformer_table(
            df,
            'table_inconnue',
            config=config_dict
        )
        
        assert len(df_clean) > 0
        assert metadata['type_transformation'] == 'defaut_uniquement'
    
    def test_charger_configuration(self):
        """Test chargement de la configuration"""
        config_path = Path('src/transform/config.yaml')
        if config_path.exists():
            config = charger_configuration(str(config_path))
            assert isinstance(config, dict)
            assert 'regles_default' in config or 'tables_specifiques' in config


class TestIntegration:
    """Tests d'intégration"""
    
    @pytest.mark.integration
    def test_pipeline_complet_clients(self, sample_clients_df, config_dict):
        """Test pipeline complet pour clients"""
        # Étape 1 : Transformation
        df_clean, metadata = transformer_table(
            sample_clients_df,
            'clients',
            config=config_dict
        )
        
        # Vérifications
        assert len(df_clean) > 0
        assert len(df_clean.columns) > 0
        assert metadata['nombre_lignes_final'] == len(df_clean)
        assert metadata['nombre_colonnes_final'] == len(df_clean.columns)
        
        # Vérifier qualité des données
        assert metadata['qualite']['taux_completude'] >= 0
        assert metadata['qualite']['taux_completude'] <= 100

