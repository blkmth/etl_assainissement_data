"""
Script de test et démonstration du système de transformation.
"""
import pandas as pd
import json
from src.transform.orchestrateur import transformer_table
from src.config.logging_config import setup_logger

logger = setup_logger(__name__)


def creer_donnees_test_clients():
    """Crée un DataFrame de test pour table 'clients'"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'nom': ['Dupont', 'Martin', 'Bernard', 'Thomas', 'Dupont'],  # Doublon ligne 5
        'prenom': ['Jean', 'Marie', 'Paul', 'Sophie', 'Jean'],
        'email': [
            'jean.dupont@example.com',
            'marie.martin@example.com',
            'invalid-email',  # Email invalide
            'sophie.thomas@example.com',
            'jean.dupont@example.com'  # Doublon
        ],
        'pays': ['France', 'États-Unis', 'Allemagne', 'United Kingdom', 'France'],
        'revenu_annuel': [50000, 75000, 60000, 15000000, 50000],  # Ligne 4 hors limite
        'depenses_annuelles': [40000, 50000, 45000, 30000, 40000],
        'solde_compte': [10000, 25000, 15000, 50000, 10000],
        'numero_carte': [
            '4532123456789012',
            '5425233430109903',
            '374245455400126',
            None,
            '4532123456789012'
        ],
        'transaction_mensuelle': [12, 8, 15, 20, 12]
    })


def creer_donnees_test_vehicules():
    """Crée un DataFrame de test pour table 'vehicules'"""
    return pd.DataFrame({
        'make': ['BMW', 'Toyota', 'Ford', 'Honda'],
        'model': ['X5', 'Camry', 'F-150', 'Civic'],
        'trim': ['Sport', 'LE', 'XLT', None],
        'body': ['SUV', 'Sedan', 'Truck', 'Sedan'],
        'transmission': ['automatic', 'automatic', 'automatic', 'manual'],
        'vin': [
            '5UXKR0C58F0P04749',
            '4T1BF1FK5CU123456',
            'INVALID_VIN',  # VIN invalide
            '19UUA56662A123456'
        ],
        'state': ['CA', 'TX', 'FL', 'NY'],
        'condition': ['excellent', 'good', 'fair', 'excellent'],
        'odometer': [25000, 50000, 75000, 15000],
        'color': ['black', 'white', 'blue', 'red'],
        'interior': ['leather', 'cloth', 'leather', 'cloth'],
        'seller': ['Dealer A', 'Dealer B', 'Private', 'Dealer C'],
        'mmr': [45000, 25000, 30000, 22000],
        'sellingprice': [47000, 26500, 28000, 23500],
        'saledate': ['2024-01-15', '2024-02-20', '2024-03-10', '2024-04-05']
    })


def afficher_metadonnees(metadata: dict):
    """Affiche les métadonnées de manière lisible"""
    print("\n" + "="*80)
    print("📊 MÉTADONNÉES DE TRANSFORMATION")
    print("="*80)
    print(json.dumps(metadata, indent=2, default=str, ensure_ascii=False))
    print("="*80 + "\n")


# ========== EXÉCUTION TESTS ==========

if __name__ == "__main__":
    
    print("\n\n")
    print("█"*80)
    print("█" + " "*78 + "█")
    print("█" + "  🧪 TEST SYSTÈME DE TRANSFORMATION MODULAIRE".center(78) + "█")
    print("█" + " "*78 + "█")
    print("█"*80)
    print("\n\n")
    
    # ===== TEST 1 : TABLE CLIENTS (TRANSFORMATION SPÉCIFIQUE) =====
    print("\n" + "🧪 TEST 1 : TRANSFORMATION TABLE CLIENTS (Spécifique)")
    print("─"*80)
    
    df_clients = creer_donnees_test_clients()
    print(f"\n📥 Données brutes (clients) : {len(df_clients)} lignes")
    print(df_clients.head())
    
    df_clients_clean, meta_clients = transformer_table(
        df_clients,
        'clients',  # ou 'finances'
        chemin_config='src/transform/config.yaml'
    )
    
    print(f"\n📤 Données transformées (clients) : {len(df_clients_clean)} lignes")
    print(df_clients_clean.head())
    
    print("\n🔒 Vérification anonymisation:")
    if 'numero_carte' in df_clients_clean.columns:
        print(df_clients_clean[['nom', 'numero_carte']].head())
    
    print("\n🌍 Vérification conversion pays:")
    if 'pays_code' in df_clients_clean.columns:
        print(df_clients_clean[['pays', 'pays_code']].head())
    
    print("\n📊 Vérification métriques calculées:")
    if 'taux_endettement' in df_clients_clean.columns:
        print(df_clients_clean[['nom', 'taux_endettement', 'solde_net']].head())
    
    afficher_metadonnees(meta_clients)
    
    # ===== TEST 2 : TABLE VEHICULES (TRANSFORMATION SPÉCIFIQUE LÉGÈRE) =====
    print("\n" + "🧪 TEST 2 : TRANSFORMATION TABLE VEHICULES (Spécifique)")
    print("─"*80)
    
    df_vehicules = creer_donnees_test_vehicules()
    print(f"\n📥 Données brutes (vehicules) : {len(df_vehicules)} lignes")
    print(df_vehicules.head())
    
    df_vehicules_clean, meta_vehicules = transformer_table(
        df_vehicules,
        'ventes',  # ou 'vehicules'
        chemin_config='src/transform/config.yaml'
    )
    
    print(f"\n📤 Données transformées (vehicules) : {len(df_vehicules_clean)} lignes")
    print(df_vehicules_clean.head())
    
    print("\n🔍 Vérification validation VIN:")
    if 'vin_valide' in df_vehicules_clean.columns:
        print(df_vehicules_clean[['make', 'model', 'vin', 'vin_valide']])
    
    afficher_metadonnees(meta_vehicules)
    
    # ===== TEST 3 : TABLE INCONNUE (TRANSFORMATION PAR DÉFAUT) =====
    print("\n" + "🧪 TEST 3 : TRANSFORMATION TABLE INCONNUE (Défaut uniquement)")
    print("─"*80)
    
    df_inconnu = pd.DataFrame({
        'ID Client': [1, 2, 3, 3],  # Doublon
        'Date  Commande': ['2024-01-01', '2024-02-15', '2024-03-20', '2024-03-20'],
        'Montant TTC': [100.50, None, 250.75, 250.75],
        'Statüt': ['Validé', 'En cours', None, None]  # Avec accent et NULL
    })
    
    print(f"\n📥 Données brutes (table_inconnue) : {len(df_inconnu)} lignes")
    print(df_inconnu)
    
    df_inconnu_clean, meta_inconnu = transformer_table(
        df_inconnu,
        'table_inconnue',
        chemin_config='src/transform/config.yaml'
    )
    
    print(f"\n📤 Données transformées (table_inconnue) : {len(df_inconnu_clean)} lignes")
    print(df_inconnu_clean)
    
    print("\n✅ Vérifications transformation par défaut:")
    print(f"  - Colonnes normalisées: {df_inconnu_clean.columns.tolist()}")
    print(f"  - Doublons supprimés: {len(df_inconnu) - len(df_inconnu_clean)}")
    print(f"  - Date convertie: {df_inconnu_clean.dtypes['date_commande']}")
    
    afficher_metadonnees(meta_inconnu)
    
    print("\n\n")
    print("█"*80)
    print("█" + " "*78 + "█")
    print("█" + "  ✅ TOUS LES TESTS TERMINÉS AVEC SUCCÈS".center(78) + "█")
    print("█" + " "*78 + "█")
    print("█"*80)
