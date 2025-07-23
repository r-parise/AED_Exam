#!/usr/bin/env python3
"""
=================================================================================
ANALYSE AML - CODE PYTHON POUR PRÉSENTATION AUX COLLÈGUES
=================================================================================

Auteur: Équipe Analyse AED Luxembourg
Date: Juillet 2025
Objectif: Démonstration de l'analyse des questionnaires anti-blanchiment

Structure du code:
1. Configuration et imports
2. Chargement et consolidation des données
3. Application des critères a-h
4. Calcul des scores de risque
5. Génération des statistiques
6. Création des visualisations
7. Export des résultats

Données traitées: 1,861 questionnaires (2018-2024)
Critères appliqués: 8 critères selon loi LBC/FT
=================================================================================
"""

# =============================================================================
# 1. CONFIGURATION ET IMPORTS
# =============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
from datetime import datetime
from pathlib import Path

# Configuration
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

print("ANALYSE AML - QUESTIONNAIRES ANTI-BLANCHIMENT")
print("=" * 60)
print("Conforme à la loi modifiée du 12 novembre 2004")
print("=" * 60)

# =============================================================================
# 2. CHARGEMENT ET CONSOLIDATION DES DONNÉES
# =============================================================================

def load_aml_data():
    """
    Charge et consolide les 5 fichiers Excel fournis par l'AED
    
    Returns:
        pd.DataFrame: Données consolidées
    """
    print("\nÉTAPE 1: Chargement des données")
    print("-" * 40)
    
    # Définir les fichiers sources
    files = {
        'master': 'aml_master.xlsx',
        'quest': 'aml_quest data.xlsx', 
        'revenue': 'aml_revenu professionnel.xlsx',
        'payment': 'aml_methode paiement client.xlsx',
        'software': 'aml_soft_check.xlsx'
    }
    
    # Charger chaque fichier
    data = {}
    for key, filename in files.items():
        try:
            if key == 'master':
                data[key] = pd.read_excel(filename, sheet_name='AML_SURVEYS')
            elif key == 'quest':
                data[key] = pd.read_excel(filename, sheet_name='quest data')
            elif key == 'revenue':
                data[key] = pd.read_excel(filename, sheet_name='revenu professionnel')
            elif key == 'payment':
                data[key] = pd.read_excel(filename, sheet_name='methode paiement client')
            elif key == 'software':
                data[key] = pd.read_excel(filename, sheet_name='aml soft check')
            
            print(f"{key.upper()}: {len(data[key]):,} lignes chargées")
            
        except Exception as e:
            print(f"Erreur {key}: {e}")
    
    # Consolidation sur SURVEY_ID
    print("\nConsolidation des données...")
    
    # Base: fichier master
    consolidated = data['master'].copy()
    
    # Joindre les données détaillées
    consolidated = consolidated.merge(
        data['quest'], on='SURVEY_ID', how='left', suffixes=('', '_quest')
    )
    
    # Agréger les revenus par professionnel
    revenue_agg = data['revenue'].groupby('SURVEY_ID').agg({
        'REVENUE_KIND': lambda x: list(x),
        '           REVENUE ': 'sum',  # Note: espace dans le nom
        'NB_TRANSACTIONS': 'sum'
    }).reset_index()
    revenue_agg.columns = ['SURVEY_ID', 'REVENUE_TYPES', 'TOTAL_REVENUE', 'TOTAL_TRANSACTIONS']
    
    consolidated = consolidated.merge(revenue_agg, on='SURVEY_ID', how='left')
    
    # Agréger les méthodes de paiement
    payment_agg = data['payment'].groupby('SURVEY_ID').agg({
        'REGION': lambda x: list(set(x)),
        'PAYMENT_METHOD': lambda x: list(set(x))
    }).reset_index()
    payment_agg.columns = ['SURVEY_ID', 'CLIENT_REGIONS', 'PAYMENT_METHODS']
    
    consolidated = consolidated.merge(payment_agg, on='SURVEY_ID', how='left')
    
    # Joindre les logiciels
    consolidated = consolidated.merge(data['software'], on='SURVEY_ID', how='left')
    
    print(f"Consolidation terminée: {len(consolidated):,} lignes, {len(consolidated.columns)} colonnes")
    
    return consolidated

# =============================================================================
# 3. APPLICATION DES CRITÈRES DE RISQUE (a-h)
# =============================================================================

def apply_aml_criteria(df):
    """
    Applique les 8 critères de risque selon la loi LBC/FT
    
    Args:
        df: DataFrame consolidé
        
    Returns:
        pd.DataFrame: Données avec critères appliqués
    """
    print("\nÉTAPE 2: Application des critères de risque")
    print("-" * 40)
    
    # CRITÈRE a) - Risque géographique
    def assess_geographic_risk(regions):
        """États tiers = risque élevé"""
        if pd.isna(regions):
            return 'Unknown'
        if isinstance(regions, str):
            regions = [regions]
        
        # Vérifier la présence de pays tiers
        non_eu_countries = [r for r in regions if str(r) not in ['LU', 'EU']]
        if non_eu_countries:
            return 'High'  # États tiers
        elif 'EU' in str(regions):
            return 'Medium'  # UE hors Luxembourg
        else:
            return 'Low'  # Luxembourg
    
    df['GEOGRAPHIC_RISK'] = df['CLIENT_REGIONS'].apply(assess_geographic_risk)
    print("Critère a) appliqué - Risque géographique")
    
    # CRITÈRE c) - Identification conforme
    df['IDENTIFICATION_COMPLIANT'] = df['CLIENT_ID_STATUS'] == 'AVANCEE'
    print("Critère c) appliqué - Identification conforme (AVANCEE)")
    
    # CRITÈRE d) - Conservation documents KYC
    df['ARCHIVING_COMPLIANT'] = df['DOCUMENT_ARCHIVING'] == '5'
    print("Critère d) appliqué - Conservation 5 ans")
    
    # CRITÈRE f) - Paiements CASH
    def has_cash_payment(methods):
        """CASH = risque élevé"""
        if pd.isna(methods):
            return False
        return 'CASH' in str(methods)
    
    df['HAS_CASH_PAYMENT'] = df['PAYMENT_METHODS'].apply(has_cash_payment)
    print("Critère f) appliqué - Paiements CASH")
    
    # CRITÈRE g) - Revenus à risque élevé
    high_risk_revenues = ['SERV_CREATION_S', 'SERV_FONCTION', 'SERV_VIRTUEL']
    
    def has_high_risk_revenue(revenue_types):
        """Services SERV_* = risque élevé"""
        if pd.isna(revenue_types):
            return False
        if isinstance(revenue_types, str):
            revenue_types = [revenue_types]
        return any(rt in high_risk_revenues for rt in revenue_types)
    
    df['HAS_HIGH_RISK_REVENUE'] = df['REVENUE_TYPES'].apply(has_high_risk_revenue)
    print("Critère g) appliqué - Revenus SERV_*")
    
    # CRITÈRE h) - Risque immobilier selon transactions
    def assess_immo_risk(revenue_types, nb_transactions):
        """IMMO_ACHAT_VENT lié au nombre de transactions"""
        if pd.isna(revenue_types) or pd.isna(nb_transactions):
            return 'Standard'
        if isinstance(revenue_types, str):
            revenue_types = [revenue_types]
        
        if 'IMMO_ACHAT_VENT' in str(revenue_types):
            if nb_transactions > 10:
                return 'Very High'
            elif nb_transactions > 5:
                return 'High'
            elif nb_transactions > 1:
                return 'Medium'
            else:
                return 'Low'
        return 'Standard'
    
    df['IMMO_RISK'] = df.apply(
        lambda x: assess_immo_risk(x['REVENUE_TYPES'], x['TOTAL_TRANSACTIONS']), 
        axis=1
    )
    print("Critère h) appliqué - Transactions immobilières")
    
    return df

# =============================================================================
# 4. CALCUL DES SCORES DE RISQUE
# =============================================================================

def calculate_risk_scores(df):
    """
    Calcule le score de risque global selon les critères
    
    Args:
        df: DataFrame avec critères appliqués
        
    Returns:
        pd.DataFrame: Données avec scores de risque
    """
    print("\nÉTAPE 3: Calcul des scores de risque")
    print("-" * 40)
    
    # Initialiser le score
    df['RISK_SCORE'] = 0
    
    # Pondération par critère
    print("Pondération des critères:")
    
    # Critère a) - Géographique
    geographic_weights = {'High': 3, 'Medium': 1, 'Low': 0, 'Unknown': 0}
    for level, weight in geographic_weights.items():
        mask = df['GEOGRAPHIC_RISK'] == level
        df.loc[mask, 'RISK_SCORE'] += weight
        if mask.sum() > 0:
            print(f"  • Géographique {level}: +{weight} pts ({mask.sum()} prof.)")
    
    # Critère f) - CASH
    cash_mask = df['HAS_CASH_PAYMENT']
    df.loc[cash_mask, 'RISK_SCORE'] += 2
    print(f"  • Paiements CASH: +2 pts ({cash_mask.sum()} prof.)")
    
    # Critère g) - Revenus à risque
    revenue_mask = df['HAS_HIGH_RISK_REVENUE']
    df.loc[revenue_mask, 'RISK_SCORE'] += 2
    print(f"  • Revenus haut risque: +2 pts ({revenue_mask.sum()} prof.)")
    
    # Critère h) - Immobilier
    immo_weights = {'Very High': 3, 'High': 2, 'Medium': 1, 'Low': 0, 'Standard': 0}
    for level, weight in immo_weights.items():
        mask = df['IMMO_RISK'] == level
        df.loc[mask, 'RISK_SCORE'] += weight
        if mask.sum() > 0 and weight > 0:
            print(f"  • Immobilier {level}: +{weight} pts ({mask.sum()} prof.)")
    
    # Pénalités non-conformité
    id_penalty = ~df['IDENTIFICATION_COMPLIANT']
    arch_penalty = ~df['ARCHIVING_COMPLIANT']
    df.loc[id_penalty, 'RISK_SCORE'] += 1
    df.loc[arch_penalty, 'RISK_SCORE'] += 1
    print(f"  • Non-conformité ID: +1 pt ({id_penalty.sum()} prof.)")
    print(f"  • Non-conformité archivage: +1 pt ({arch_penalty.sum()} prof.)")
    
    # Classification finale
    df['RISK_LEVEL'] = pd.cut(
        df['RISK_SCORE'],
        bins=[-1, 0, 2, 4, float('inf')],
        labels=['Low', 'Medium', 'High', 'Critical']
    )
    
    # Afficher la distribution
    risk_distribution = df['RISK_LEVEL'].value_counts()
    print(f"\nDistribution des niveaux de risque:")
    for level in ['Low', 'Medium', 'High', 'Critical']:
        count = risk_distribution.get(level, 0)
        pct = count / len(df) * 100
        print(f"  • {level}: {count:,} ({pct:.1f}%)")
    
    return df

# =============================================================================
# 5. GÉNÉRATION DES STATISTIQUES CLÉS
# =============================================================================

def generate_key_statistics(df):
    """
    Génère les statistiques principales pour le rapport
    
    Args:
        df: DataFrame analysé
        
    Returns:
        dict: Dictionnaire des statistiques
    """
    print("\nÉTAPE 4: Génération des statistiques")
    print("-" * 40)
    
    stats = {}
    
    # Vue d'ensemble
    stats['overview'] = {
        'total_questionnaires': len(df),
        'periode': f"{df['DEC_YEAR'].min()}-{df['DEC_YEAR'].max()}",
        'secteurs': df['SECTOR'].value_counts().to_dict()
    }
    
    # Conformité
    stats['conformity'] = {
        'taux_identification': df['IDENTIFICATION_COMPLIANT'].mean() * 100,
        'taux_archivage': df['ARCHIVING_COMPLIANT'].mean() * 100,
        'compliance_officer': (df['COMPLIANCE_OFFICER'] == 'X').mean() * 100
    }
    
    # Risques
    stats['risks'] = {
        'critiques': (df['RISK_LEVEL'] == 'Critical').sum(),
        'eleves': (df['RISK_LEVEL'] == 'High').sum(),
        'cash_users': df['HAS_CASH_PAYMENT'].sum(),
        'pays_tiers': (df['GEOGRAPHIC_RISK'] == 'High').sum(),
        'revenus_risque': df['HAS_HIGH_RISK_REVENUE'].sum()
    }
    
    # Par secteur
    stats['sectors'] = {}
    for sector in df['SECTOR'].unique():
        sector_data = df[df['SECTOR'] == sector]
        stats['sectors'][sector] = {
            'count': len(sector_data),
            'risk_score_avg': sector_data['RISK_SCORE'].mean(),
            'conformity_rate': sector_data['IDENTIFICATION_COMPLIANT'].mean() * 100,
            'critical_count': (sector_data['RISK_LEVEL'] == 'Critical').sum()
        }
    
    # Afficher résumé
    print("RÉSULTATS CLÉS:")
    print(f"  • Total questionnaires: {stats['overview']['total_questionnaires']:,}")
    print(f"  • Taux identification: {stats['conformity']['taux_identification']:.1f}%")
    print(f"  • Professionnels critiques: {stats['risks']['critiques']:,}")
    print(f"  • Utilisateurs CASH: {stats['risks']['cash_users']:,}")
    
    return stats

# =============================================================================
# 6. CRÉATION DES VISUALISATIONS
# =============================================================================

def create_key_visualizations(df, stats):
    """
    Crée les visualisations principales
    
    Args:
        df: DataFrame analysé
        stats: Statistiques calculées
        
    Returns:
        dict: Figures Plotly
    """
    print("\nÉTAPE 5: Création des visualisations")
    print("-" * 40)
    
    figures = {}
    
    # 1. Distribution des secteurs
    sector_data = df['SECTOR'].value_counts()
    figures['sectors'] = px.pie(
        values=sector_data.values,
        names=sector_data.index,
        title="Répartition par Secteur",
        color_discrete_sequence=['#3498db', '#2ecc71', '#e67e22']
    )
    print("Graphique secteurs créé")
    
    # 2. Niveaux de risque
    risk_data = df['RISK_LEVEL'].value_counts()
    colors = {'Low': '#27ae60', 'Medium': '#f39c12', 'High': '#e74c3c', 'Critical': '#8e44ad'}
    figures['risk_levels'] = px.bar(
        x=risk_data.index,
        y=risk_data.values,
        title="Distribution des Niveaux de Risque",
        color=risk_data.index,
        color_discrete_map=colors
    )
    print("Graphique niveaux de risque créé")
    
    # 3. Conformité par secteur
    compliance_data = df.groupby('SECTOR')['IDENTIFICATION_COMPLIANT'].mean() * 100
    figures['compliance'] = px.bar(
        x=compliance_data.index,
        y=compliance_data.values,
        title="Taux de Conformité Identification par Secteur (%)",
        text=[f"{rate:.1f}%" for rate in compliance_data.values]
    )
    figures['compliance'].add_hline(y=90, line_dash="dash", line_color="red", 
                                   annotation_text="Objectif 90%")
    print("Graphique conformité créé")
    
    # 4. Évolution temporelle
    yearly_data = df.groupby('DEC_YEAR').size()
    figures['evolution'] = px.line(
        x=yearly_data.index,
        y=yearly_data.values,
        title="Évolution du Nombre de Questionnaires",
        markers=True
    )
    print("Graphique évolution créé")
    
    return figures

# =============================================================================
# 7. EXPORT DES RÉSULTATS
# =============================================================================

def export_results(df, stats, figures):
    """
    Exporte les résultats dans différents formats
    
    Args:
        df: DataFrame analysé
        stats: Statistiques
        figures: Visualisations
    """
    print("\nÉTAPE 6: Export des résultats")
    print("-" * 40)
    
    # 1. Excel avec multiple feuilles
    with pd.ExcelWriter('resultats_aml_analyse.xlsx', engine='openpyxl') as writer:
        # Données complètes
        df.to_excel(writer, sheet_name='Données Complètes', index=False)
        
        # Résumé exécutif
        resume_data = [
            ['Total questionnaires', stats['overview']['total_questionnaires']],
            ['Taux identification (%)', f"{stats['conformity']['taux_identification']:.1f}"],
            ['Taux archivage (%)', f"{stats['conformity']['taux_archivage']:.1f}"],
            ['Professionnels critiques', stats['risks']['critiques']],
            ['Utilisateurs CASH', stats['risks']['cash_users']],
            ['Clients pays tiers', stats['risks']['pays_tiers']]
        ]
        pd.DataFrame(resume_data, columns=['Indicateur', 'Valeur']).to_excel(
            writer, sheet_name='Résumé Exécutif', index=False
        )
        
        # Alertes critiques
        alertes = df[df['RISK_LEVEL'] == 'Critical'][
            ['SURVEY_ID', 'SECTOR', 'RISK_SCORE', 'GEOGRAPHIC_RISK', 
             'HAS_CASH_PAYMENT', 'HAS_HIGH_RISK_REVENUE']
        ]
        alertes.to_excel(writer, sheet_name='Alertes Critiques', index=False)
    
    print("Fichier Excel exporté: resultats_aml_analyse.xlsx")
    
    # 2. Rapport HTML avec graphiques
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Rapport AML - Analyse des Questionnaires</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1 {{ color: #2c3e50; }}
            .kpi {{ background: #ecf0f1; padding: 20px; margin: 10px 0; border-radius: 5px; }}
            .chart {{ margin: 30px 0; }}
        </style>
    </head>
    <body>
        <h1>Rapport d'Analyse AML</h1>
        <div class="kpi">
            <h3>Indicateurs Clés</h3>
            <ul>
                <li>Total questionnaires: {stats['overview']['total_questionnaires']:,}</li>
                <li>Taux identification: {stats['conformity']['taux_identification']:.1f}%</li>
                <li>Professionnels critiques: {stats['risks']['critiques']:,}</li>
                <li>Utilisateurs CASH: {stats['risks']['cash_users']:,}</li>
            </ul>
        </div>
        
        <div class="chart" id="chart-sectors"></div>
        <div class="chart" id="chart-risk"></div>
        <div class="chart" id="chart-compliance"></div>
        <div class="chart" id="chart-evolution"></div>
        
        <script>
    """
    
    # Ajouter les graphiques
    for chart_id, fig in figures.items():
        html_content += f"Plotly.newPlot('chart-{chart_id}', {fig.to_json()});\n"
    
    html_content += """
        </script>
    </body>
    </html>
    """
    
    with open('rapport_aml_interactif.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print("Rapport HTML exporté: rapport_aml_interactif.html")

# =============================================================================
# 8. FONCTION PRINCIPALE D'EXÉCUTION
# =============================================================================

def main():
    """
    Fonction principale - Orchestre toute l'analyse
    """
    try:
        # Étape 1: Charger et consolider
        df = load_aml_data()
        
        # Étape 2: Appliquer les critères
        df = apply_aml_criteria(df)
        
        # Étape 3: Calculer les scores
        df = calculate_risk_scores(df)
        
        # Étape 4: Générer les statistiques
        stats = generate_key_statistics(df)
        
        # Étape 5: Créer les visualisations
        figures = create_key_visualizations(df, stats)
        
        # Étape 6: Exporter les résultats
        export_results(df, stats, figures)
        
        print("\n" + "=" * 60)
        print("ANALYSE TERMINÉE AVEC SUCCÈS!")
        print("=" * 60)
        print("\nFichiers générés:")
        print("resultats_aml_analyse.xlsx - Données et statistiques")
        print("rapport_aml_interactif.html - Rapport avec graphiques")
        print("\nPrêt pour présentation aux collègues!")
        
        return df, stats, figures
        
    except Exception as e:
        print(f"\nERREUR: {e}")
        return None, None, None

# =============================================================================
# EXÉCUTION
# =============================================================================

if __name__ == "__main__":
    # Lancer l'analyse complète
    donnees, statistiques, graphiques = main()
    
    # Afficher quelques résultats pour vérification
    if donnees is not None:
        print(f"\nVÉRIFICATION RAPIDE:")
        print(f"Données chargées: {len(donnees):,} lignes")
        print(f"Professionnels critiques: {(donnees['RISK_LEVEL'] == 'Critical').sum()}")
        print(f"Taux conformité: {donnees['IDENTIFICATION_COMPLIANT'].mean()*100:.1f}%")#!/usr/bin/env python3
"""
=================================================================================
ANALYSE AML - CODE PYTHON POUR PRÉSENTATION AUX COLLÈGUES
=================================================================================

Auteur: Équipe Analyse AED Luxembourg
Date: Juillet 2025
Objectif: Démonstration de l'analyse des questionnaires anti-blanchiment

Structure du code:
1. Configuration et imports
2. Chargement et consolidation des données
3. Application des critères a-h
4. Calcul des scores de risque
5. Génération des statistiques
6. Création des visualisations
7. Export des résultats

Données traitées: 1,861 questionnaires (2018-2024)
Critères appliqués: 8 critères selon loi LBC/FT
=================================================================================
"""

# =============================================================================
# 1. CONFIGURATION ET IMPORTS
# =============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
from datetime import datetime
from pathlib import Path

# Configuration
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

print("🔍 ANALYSE AML - QUESTIONNAIRES ANTI-BLANCHIMENT")
print("=" * 60)
print("Conforme à la loi modifiée du 12 novembre 2004")
print("=" * 60)

# =============================================================================
# 2. CHARGEMENT ET CONSOLIDATION DES DONNÉES
# =============================================================================

def load_aml_data():
    """
    Charge et consolide les 5 fichiers Excel fournis par l'AED
    
    Returns:
        pd.DataFrame: Données consolidées
    """
    print("\n📁 ÉTAPE 1: Chargement des données")
    print("-" * 40)
    
    # Définir les fichiers sources
    files = {
        'master': 'aml_master.xlsx',
        'quest': 'aml_quest data.xlsx', 
        'revenue': 'aml_revenu professionnel.xlsx',
        'payment': 'aml_methode paiement client.xlsx',
        'software': 'aml_soft_check.xlsx'
    }
    
    # Charger chaque fichier
    data = {}
    for key, filename in files.items():
        try:
            if key == 'master':
                data[key] = pd.read_excel(filename, sheet_name='AML_SURVEYS')
            elif key == 'quest':
                data[key] = pd.read_excel(filename, sheet_name='quest data')
            elif key == 'revenue':
                data[key] = pd.read_excel(filename, sheet_name='revenu professionnel')
            elif key == 'payment':
                data[key] = pd.read_excel(filename, sheet_name='methode paiement client')
            elif key == 'software':
                data[key] = pd.read_excel(filename, sheet_name='aml soft check')
            
            print(f"✅ {key.upper()}: {len(data[key]):,} lignes chargées")
            
        except Exception as e:
            print(f"❌ Erreur {key}: {e}")
    
    # Consolidation sur SURVEY_ID
    print("\n🔗 Consolidation des données...")
    
    # Base: fichier master
    consolidated = data['master'].copy()
    
    # Joindre les données détaillées
    consolidated = consolidated.merge(
        data['quest'], on='SURVEY_ID', how='left', suffixes=('', '_quest')
    )
    
    # Agréger les revenus par professionnel
    revenue_agg = data['revenue'].groupby('SURVEY_ID').agg({
        'REVENUE_KIND': lambda x: list(x),
        '           REVENUE ': 'sum',  # Note: espace dans le nom
        'NB_TRANSACTIONS': 'sum'
    }).reset_index()
    revenue_agg.columns = ['SURVEY_ID', 'REVENUE_TYPES', 'TOTAL_REVENUE', 'TOTAL_TRANSACTIONS']
    
    consolidated = consolidated.merge(revenue_agg, on='SURVEY_ID', how='left')
    
    # Agréger les méthodes de paiement
    payment_agg = data['payment'].groupby('SURVEY_ID').agg({
        'REGION': lambda x: list(set(x)),
        'PAYMENT_METHOD': lambda x: list(set(x))
    }).reset_index()
    payment_agg.columns = ['SURVEY_ID', 'CLIENT_REGIONS', 'PAYMENT_METHODS']
    
    consolidated = consolidated.merge(payment_agg, on='SURVEY_ID', how='left')
    
    # Joindre les logiciels
    consolidated = consolidated.merge(data['software'], on='SURVEY_ID', how='left')
    
    print(f"✅ Consolidation terminée: {len(consolidated):,} lignes, {len(consolidated.columns)} colonnes")
    
    return consolidated

# =============================================================================
# 3. APPLICATION DES CRITÈRES DE RISQUE (a-h)
# =============================================================================

def apply_aml_criteria(df):
    """
    Applique les 8 critères de risque selon la loi LBC/FT
    
    Args:
        df: DataFrame consolidé
        
    Returns:
        pd.DataFrame: Données avec critères appliqués
    """
    print("\n⚖️ ÉTAPE 2: Application des critères de risque")
    print("-" * 40)
    
    # CRITÈRE a) - Risque géographique
    def assess_geographic_risk(regions):
        """États tiers = risque élevé"""
        if pd.isna(regions):
            return 'Unknown'
        if isinstance(regions, str):
            regions = [regions]
        
        # Vérifier la présence de pays tiers
        non_eu_countries = [r for r in regions if str(r) not in ['LU', 'EU']]
        if non_eu_countries:
            return 'High'  # États tiers
        elif 'EU' in str(regions):
            return 'Medium'  # UE hors Luxembourg
        else:
            return 'Low'  # Luxembourg
    
    df['GEOGRAPHIC_RISK'] = df['CLIENT_REGIONS'].apply(assess_geographic_risk)
    print("✅ Critère a) appliqué - Risque géographique")
    
    # CRITÈRE c) - Identification conforme
    df['IDENTIFICATION_COMPLIANT'] = df['CLIENT_ID_STATUS'] == 'AVANCEE'
    print("✅ Critère c) appliqué - Identification conforme (AVANCEE)")
    
    # CRITÈRE d) - Conservation documents KYC
    df['ARCHIVING_COMPLIANT'] = df['DOCUMENT_ARCHIVING'] == '5'
    print("✅ Critère d) appliqué - Conservation 5 ans")
    
    # CRITÈRE f) - Paiements CASH
    def has_cash_payment(methods):
        """CASH = risque élevé"""
        if pd.isna(methods):
            return False
        return 'CASH' in str(methods)
    
    df['HAS_CASH_PAYMENT'] = df['PAYMENT_METHODS'].apply(has_cash_payment)
    print("✅ Critère f) appliqué - Paiements CASH")
    
    # CRITÈRE g) - Revenus à risque élevé
    high_risk_revenues = ['SERV_CREATION_S', 'SERV_FONCTION', 'SERV_VIRTUEL']
    
    def has_high_risk_revenue(revenue_types):
        """Services SERV_* = risque élevé"""
        if pd.isna(revenue_types):
            return False
        if isinstance(revenue_types, str):
            revenue_types = [revenue_types]
        return any(rt in high_risk_revenues for rt in revenue_types)
    
    df['HAS_HIGH_RISK_REVENUE'] = df['REVENUE_TYPES'].apply(has_high_risk_revenue)
    print("✅ Critère g) appliqué - Revenus SERV_*")
    
    # CRITÈRE h) - Risque immobilier selon transactions
    def assess_immo_risk(revenue_types, nb_transactions):
        """IMMO_ACHAT_VENT lié au nombre de transactions"""
        if pd.isna(revenue_types) or pd.isna(nb_transactions):
            return 'Standard'
        if isinstance(revenue_types, str):
            revenue_types = [revenue_types]
        
        if 'IMMO_ACHAT_VENT' in str(revenue_types):
            if nb_transactions > 10:
                return 'Very High'
            elif nb_transactions > 5:
                return 'High'
            elif nb_transactions > 1:
                return 'Medium'
            else:
                return 'Low'
        return 'Standard'
    
    df['IMMO_RISK'] = df.apply(
        lambda x: assess_immo_risk(x['REVENUE_TYPES'], x['TOTAL_TRANSACTIONS']), 
        axis=1
    )
    print("✅ Critère h) appliqué - Transactions immobilières")
    
    return df

# =============================================================================
# 4. CALCUL DES SCORES DE RISQUE
# =============================================================================

def calculate_risk_scores(df):
    """
    Calcule le score de risque global selon les critères
    
    Args:
        df: DataFrame avec critères appliqués
        
    Returns:
        pd.DataFrame: Données avec scores de risque
    """
    print("\n🎯 ÉTAPE 3: Calcul des scores de risque")
    print("-" * 40)
    
    # Initialiser le score
    df['RISK_SCORE'] = 0
    
    # Pondération par critère
    print("Pondération des critères:")
    
    # Critère a) - Géographique
    geographic_weights = {'High': 3, 'Medium': 1, 'Low': 0, 'Unknown': 0}
    for level, weight in geographic_weights.items():
        mask = df['GEOGRAPHIC_RISK'] == level
        df.loc[mask, 'RISK_SCORE'] += weight
        if mask.sum() > 0:
            print(f"  • Géographique {level}: +{weight} pts ({mask.sum()} prof.)")
    
    # Critère f) - CASH
    cash_mask = df['HAS_CASH_PAYMENT']
    df.loc[cash_mask, 'RISK_SCORE'] += 2
    print(f"  • Paiements CASH: +2 pts ({cash_mask.sum()} prof.)")
    
    # Critère g) - Revenus à risque
    revenue_mask = df['HAS_HIGH_RISK_REVENUE']
    df.loc[revenue_mask, 'RISK_SCORE'] += 2
    print(f"  • Revenus haut risque: +2 pts ({revenue_mask.sum()} prof.)")
    
    # Critère h) - Immobilier
    immo_weights = {'Very High': 3, 'High': 2, 'Medium': 1, 'Low': 0, 'Standard': 0}
    for level, weight in immo_weights.items():
        mask = df['IMMO_RISK'] == level
        df.loc[mask, 'RISK_SCORE'] += weight
        if mask.sum() > 0 and weight > 0:
            print(f"  • Immobilier {level}: +{weight} pts ({mask.sum()} prof.)")
    
    # Pénalités non-conformité
    id_penalty = ~df['IDENTIFICATION_COMPLIANT']
    arch_penalty = ~df['ARCHIVING_COMPLIANT']
    df.loc[id_penalty, 'RISK_SCORE'] += 1
    df.loc[arch_penalty, 'RISK_SCORE'] += 1
    print(f"  • Non-conformité ID: +1 pt ({id_penalty.sum()} prof.)")
    print(f"  • Non-conformité archivage: +1 pt ({arch_penalty.sum()} prof.)")
    
    # Classification finale
    df['RISK_LEVEL'] = pd.cut(
        df['RISK_SCORE'],
        bins=[-1, 0, 2, 4, float('inf')],
        labels=['Low', 'Medium', 'High', 'Critical']
    )
    
    # Afficher la distribution
    risk_distribution = df['RISK_LEVEL'].value_counts()
    print(f"\n📊 Distribution des niveaux de risque:")
    for level in ['Low', 'Medium', 'High', 'Critical']:
        count = risk_distribution.get(level, 0)
        pct = count / len(df) * 100
        print(f"  • {level}: {count:,} ({pct:.1f}%)")
    
    return df

# =============================================================================
# 5. GÉNÉRATION DES STATISTIQUES CLÉS
# =============================================================================

def generate_key_statistics(df):
    """
    Génère les statistiques principales pour le rapport
    
    Args:
        df: DataFrame analysé
        
    Returns:
        dict: Dictionnaire des statistiques
    """
    print("\n📈 ÉTAPE 4: Génération des statistiques")
    print("-" * 40)
    
    stats = {}
    
    # Vue d'ensemble
    stats['overview'] = {
        'total_questionnaires': len(df),
        'periode': f"{df['DEC_YEAR'].min()}-{df['DEC_YEAR'].max()}",
        'secteurs': df['SECTOR'].value_counts().to_dict()
    }
    
    # Conformité
    stats['conformity'] = {
        'taux_identification': df['IDENTIFICATION_COMPLIANT'].mean() * 100,
        'taux_archivage': df['ARCHIVING_COMPLIANT'].mean() * 100,
        'compliance_officer': (df['COMPLIANCE_OFFICER'] == 'X').mean() * 100
    }
    
    # Risques
    stats['risks'] = {
        'critiques': (df['RISK_LEVEL'] == 'Critical').sum(),
        'eleves': (df['RISK_LEVEL'] == 'High').sum(),
        'cash_users': df['HAS_CASH_PAYMENT'].sum(),
        'pays_tiers': (df['GEOGRAPHIC_RISK'] == 'High').sum(),
        'revenus_risque': df['HAS_HIGH_RISK_REVENUE'].sum()
    }
    
    # Par secteur
    stats['sectors'] = {}
    for sector in df['SECTOR'].unique():
        sector_data = df[df['SECTOR'] == sector]
        stats['sectors'][sector] = {
            'count': len(sector_data),
            'risk_score_avg': sector_data['RISK_SCORE'].mean(),
            'conformity_rate': sector_data['IDENTIFICATION_COMPLIANT'].mean() * 100,
            'critical_count': (sector_data['RISK_LEVEL'] == 'Critical').sum()
        }
    
    # Afficher résumé
    print("📋 RÉSULTATS CLÉS:")
    print(f"  • Total questionnaires: {stats['overview']['total_questionnaires']:,}")
    print(f"  • Taux identification: {stats['conformity']['taux_identification']:.1f}%")
    print(f"  • Professionnels critiques: {stats['risks']['critiques']:,}")
    print(f"  • Utilisateurs CASH: {stats['risks']['cash_users']:,}")
    
    return stats

# =============================================================================
# 6. CRÉATION DES VISUALISATIONS
# =============================================================================

def create_key_visualizations(df, stats):
    """
    Crée les visualisations principales
    
    Args:
        df: DataFrame analysé
        stats: Statistiques calculées
        
    Returns:
        dict: Figures Plotly
    """
    print("\n📊 ÉTAPE 5: Création des visualisations")
    print("-" * 40)
    
    figures = {}
    
    # 1. Distribution des secteurs
    sector_data = df['SECTOR'].value_counts()
    figures['sectors'] = px.pie(
        values=sector_data.values,
        names=sector_data.index,
        title="Répartition par Secteur",
        color_discrete_sequence=['#3498db', '#2ecc71', '#e67e22']
    )
    print("✅ Graphique secteurs créé")
    
    # 2. Niveaux de risque
    risk_data = df['RISK_LEVEL'].value_counts()
    colors = {'Low': '#27ae60', 'Medium': '#f39c12', 'High': '#e74c3c', 'Critical': '#8e44ad'}
    figures['risk_levels'] = px.bar(
        x=risk_data.index,
        y=risk_data.values,
        title="Distribution des Niveaux de Risque",
        color=risk_data.index,
        color_discrete_map=colors
    )
    print("✅ Graphique niveaux de risque créé")
    
    # 3. Conformité par secteur
    compliance_data = df.groupby('SECTOR')['IDENTIFICATION_COMPLIANT'].mean() * 100
    figures['compliance'] = px.bar(
        x=compliance_data.index,
        y=compliance_data.values,
        title="Taux de Conformité Identification par Secteur (%)",
        text=[f"{rate:.1f}%" for rate in compliance_data.values]
    )
    figures['compliance'].add_hline(y=90, line_dash="dash", line_color="red", 
                                   annotation_text="Objectif 90%")
    print("✅ Graphique conformité créé")
    
    # 4. Évolution temporelle
    yearly_data = df.groupby('DEC_YEAR').size()
    figures['evolution'] = px.line(
        x=yearly_data.index,
        y=yearly_data.values,
        title="Évolution du Nombre de Questionnaires",
        markers=True
    )
    print("✅ Graphique évolution créé")
    
    return figures

# =============================================================================
# 7. EXPORT DES RÉSULTATS
# =============================================================================

def export_results(df, stats, figures):
    """
    Exporte les résultats dans différents formats
    
    Args:
        df: DataFrame analysé
        stats: Statistiques
        figures: Visualisations
    """
    print("\n💾 ÉTAPE 6: Export des résultats")
    print("-" * 40)
    
    # 1. Excel avec multiple feuilles
    with pd.ExcelWriter('resultats_aml_analyse.xlsx', engine='openpyxl') as writer:
        # Données complètes
        df.to_excel(writer, sheet_name='Données Complètes', index=False)
        
        # Résumé exécutif
        resume_data = [
            ['Total questionnaires', stats['overview']['total_questionnaires']],
            ['Taux identification (%)', f"{stats['conformity']['taux_identification']:.1f}"],
            ['Taux archivage (%)', f"{stats['conformity']['taux_archivage']:.1f}"],
            ['Professionnels critiques', stats['risks']['critiques']],
            ['Utilisateurs CASH', stats['risks']['cash_users']],
            ['Clients pays tiers', stats['risks']['pays_tiers']]
        ]
        pd.DataFrame(resume_data, columns=['Indicateur', 'Valeur']).to_excel(
            writer, sheet_name='Résumé Exécutif', index=False
        )
        
        # Alertes critiques
        alertes = df[df['RISK_LEVEL'] == 'Critical'][
            ['SURVEY_ID', 'SECTOR', 'RISK_SCORE', 'GEOGRAPHIC_RISK', 
             'HAS_CASH_PAYMENT', 'HAS_HIGH_RISK_REVENUE']
        ]
        alertes.to_excel(writer, sheet_name='Alertes Critiques', index=False)
    
    print("✅ Fichier Excel exporté: resultats_aml_analyse.xlsx")
    
    # 2. Rapport HTML avec graphiques
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Rapport AML - Analyse des Questionnaires</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1 {{ color: #2c3e50; }}
            .kpi {{ background: #ecf0f1; padding: 20px; margin: 10px 0; border-radius: 5px; }}
            .chart {{ margin: 30px 0; }}
        </style>
    </head>
    <body>
        <h1>📊 Rapport d'Analyse AML</h1>
        <div class="kpi">
            <h3>Indicateurs Clés</h3>
            <ul>
                <li>Total questionnaires: {stats['overview']['total_questionnaires']:,}</li>
                <li>Taux identification: {stats['conformity']['taux_identification']:.1f}%</li>
                <li>Professionnels critiques: {stats['risks']['critiques']:,}</li>
                <li>Utilisateurs CASH: {stats['risks']['cash_users']:,}</li>
            </ul>
        </div>
        
        <div class="chart" id="chart-sectors"></div>
        <div class="chart" id="chart-risk"></div>
        <div class="chart" id="chart-compliance"></div>
        <div class="chart" id="chart-evolution"></div>
        
        <script>
    """
    
    # Ajouter les graphiques
    for chart_id, fig in figures.items():
        html_content += f"Plotly.newPlot('chart-{chart_id}', {fig.to_json()});\n"
    
    html_content += """
        </script>
    </body>
    </html>
    """
    
    with open('rapport_aml_interactif.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print("✅ Rapport HTML exporté: rapport_aml_interactif.html")

# =============================================================================
# 8. FONCTION PRINCIPALE D'EXÉCUTION
# =============================================================================

def main():
    """
    Fonction principale - Orchestre toute l'analyse
    """
    try:
        # Étape 1: Charger et consolider
        df = load_aml_data()
        
        # Étape 2: Appliquer les critères
        df = apply_aml_criteria(df)
        
        # Étape 3: Calculer les scores
        df = calculate_risk_scores(df)
        
        # Étape 4: Générer les statistiques
        stats = generate_key_statistics(df)
        
        # Étape 5: Créer les visualisations
        figures = create_key_visualizations(df, stats)
        
        # Étape 6: Exporter les résultats
        export_results(df, stats, figures)
        
        print("\n" + "=" * 60)
        print("✅ ANALYSE TERMINÉE AVEC SUCCÈS!")
        print("=" * 60)
        print("\nFichiers générés:")
        print("📊 resultats_aml_analyse.xlsx - Données et statistiques")
        print("🌐 rapport_aml_interactif.html - Rapport avec graphiques")
        print("\nPrêt pour présentation aux collègues! 🎉")
        
        return df, stats, figures
        
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        return None, None, None

# =============================================================================
# EXÉCUTION
# =============================================================================

if __name__ == "__main__":
    # Lancer l'analyse complète
    donnees, statistiques, graphiques = main()
    
    # Afficher quelques résultats pour vérification
    if donnees is not None:
        print(f"\n🔍 VÉRIFICATION RAPIDE:")
        print(f"Données chargées: {len(donnees):,} lignes")
        print(f"Professionnels critiques: {(donnees['RISK_LEVEL'] == 'Critical').sum()}")
        print(f"Taux conformité: {donnees['IDENTIFICATION_COMPLIANT'].mean()*100:.1f}%")
