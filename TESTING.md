# 🧪 Guide de Test - ETL Assainissement Data

## 📋 Vue d'ensemble

Ce projet utilise **pytest** pour les tests unitaires et d'intégration. Il existe actuellement deux types de tests :

1. **Script de démonstration** (`tests/test.py`) - Tests manuels avec affichage détaillé
2. **Tests pytest** (à créer) - Tests automatisés standards

---

## 🚀 Méthode 1 : Script de Démonstration (Actuel)

Le fichier `tests/test.py` est un script de démonstration qui teste le système de transformation avec des données de test.

### Lancer le script de test :

```bash
# Depuis la racine du projet
python tests/test.py
```

**Ou avec Poetry :**
```bash
poetry run python tests/test.py
```

**Ce que fait ce script :**
- ✅ Test 1 : Transformation table "clients" (spécifique)
- ✅ Test 2 : Transformation table "vehicules" (spécifique)
- ✅ Test 3 : Transformation table inconnue (par défaut)

---

## 🧪 Méthode 2 : Tests Pytest (Recommandé)

### Prérequis

Assurez-vous d'avoir installé les dépendances :

```bash
# Avec Poetry (recommandé)
poetry install

# Ou avec pip
pip install -r requirements.txt  # Si vous avez un fichier requirements.txt
```

### Configuration Pytest

Ajoutez cette configuration dans `pyproject.toml` :

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py", "*_test.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "-v",                    # Verbose
    "--strict-markers",      # Strict sur les markers
    "--tb=short",            # Format court des erreurs
    "--cov=src",             # Couverture de code
    "--cov-report=term-missing",  # Rapport de couverture
    "--cov-report=html"      # Rapport HTML
]
```

### Créer des tests pytest

Créez des fichiers de test dans le dossier `tests/` avec le préfixe `test_` :

**Exemple : `tests/test_transform.py`**

```python
import pytest
import pandas as pd
from src.transform.orchestrateur import transformer_table

def test_transformer_table_clients():
    """Test transformation spécifique pour table clients"""
    df = pd.DataFrame({
        'id': [1, 2],
        'nom': ['Dupont', 'Martin'],
        'email': ['test@example.com', 'test2@example.com'],
        'pays': ['France', 'USA'],
        'revenu_annuel': [50000, 75000],
        'depenses_annuelles': [40000, 50000],
        'numero_carte': ['4532123456789012', '5425233430109903']
    })
    
    df_clean, metadata = transformer_table(
        df, 
        'clients',
        chemin_config='src/transform/config.yaml'
    )
    
    assert len(df_clean) > 0
    assert 'numero_carte' in df_clean.columns
    assert metadata['type_transformation'] == 'specifique'
```

### Lancer les tests pytest

```bash
# Tous les tests
pytest

# Avec Poetry
poetry run pytest

# Tests spécifiques
pytest tests/test_transform.py

# Tests avec couverture de code
pytest --cov=src --cov-report=html

# Tests en mode verbose
pytest -v

# Tests avec affichage des print
pytest -s

# Un test spécifique
pytest tests/test_transform.py::test_transformer_table_clients
```

---

## 📊 Rapports de Couverture

Après avoir lancé les tests avec `--cov`, vous pouvez voir :

1. **Rapport dans le terminal** : `pytest --cov=src --cov-report=term-missing`
2. **Rapport HTML** : `pytest --cov=src --cov-report=html`
   - Ouvrez `htmlcov/index.html` dans votre navigateur

---

## 🔧 Structure Recommandée des Tests

```
tests/
├── __init__.py
├── conftest.py              # Fixtures pytest partagées
├── test_transform.py        # Tests des transformations
├── test_extract.py          # Tests d'extraction
├── test_load.py             # Tests de chargement
├── test_orchestrateur.py    # Tests de l'orchestrateur
└── fixtures/                # Données de test
    ├── sample_clients.csv
    └── sample_vehicules.csv
```

---

## 📝 Exemple de conftest.py

Créez `tests/conftest.py` pour partager des fixtures :

```python
import pytest
import pandas as pd

@pytest.fixture
def sample_clients_df():
    """Fixture pour DataFrame clients de test"""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'nom': ['Dupont', 'Martin', 'Bernard'],
        'email': ['test1@example.com', 'test2@example.com', 'test3@example.com'],
        'pays': ['France', 'USA', 'Allemagne'],
        'revenu_annuel': [50000, 75000, 60000],
        'depenses_annuelles': [40000, 50000, 45000],
        'numero_carte': ['4532123456789012', '5425233430109903', '374245455400126']
    })

@pytest.fixture
def config_dict():
    """Fixture pour configuration de test"""
    import yaml
    from pathlib import Path
    
    config_path = Path('src/transform/config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)
```

---

## ✅ Checklist pour les Tests

- [ ] Créer des tests unitaires pour chaque fonction
- [ ] Créer des tests d'intégration pour les workflows complets
- [ ] Tester les cas limites (données manquantes, formats invalides)
- [ ] Tester les erreurs et exceptions
- [ ] Maintenir une couverture de code > 80%
- [ ] Documenter les tests avec des docstrings

---

## 🐛 Dépannage

### Erreur : Module non trouvé
```bash
# Assurez-vous d'être dans le bon environnement
poetry shell
# ou
source venv/bin/activate
```

### Erreur : Configuration YAML non trouvée
```bash
# Vérifiez que vous êtes à la racine du projet
pwd
# Devrait afficher : /home/charles-nguessan/etl_assainissement_data
```

### Erreur : Dépendances manquantes
```bash
poetry install
# ou
pip install -r requirements.txt
```

---

## 📚 Ressources

- [Documentation Pytest](https://docs.pytest.org/)
- [Pytest avec Poetry](https://python-poetry.org/docs/managing-environments/)
- [Couverture de code avec pytest-cov](https://pytest-cov.readthedocs.io/)

