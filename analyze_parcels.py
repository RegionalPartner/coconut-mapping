#!/usr/bin/env python3
"""
ANALYSE PARCELLAIRE - Coconut Mapping Guadeloupe
Croise les parcelles RPG avec la classification satellite pour identifier
les zones a potentiel de developpement cocotier.
"""

import geopandas as gpd
import rasterio
import numpy as np
import json
import folium
from rasterstats import zonal_stats
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path(__file__).parent
RPG_DIR = (BASE_DIR / 'RPG_3-0__GPKG_RGAF09UTM20_R01_2024-01-01' / 'RPG'
           / '1_DONNEES_LIVRAISON_2024'
           / 'RPG_3-0__GPKG_RGAF09UTM20_R01_2024-01-01')
OUTPUT_DIR = BASE_DIR / 'output_imagery'
GEOTIFF_PATH = BASE_DIR / 'Guadeloupe_S2_2024_Real.tif'

# Bandes du GeoTIFF (1-indexed)
BAND_NDVI = 5
BAND_EVI = 6

# CRS
TARGET_CRS = 'EPSG:4326'

# Codes culture RPG
CODES_EXISTANT_COCOTIER = ['NOX']
CODES_HAUTE_DISPO = ['JAC', 'SNE']       # Jachere, non exploite
CODES_MOYENNE_DISPO = ['PPH', 'PTR', 'SPH']  # Paturages
CODES_NON_CONVERTIBLE = ['BEF', 'BCA']   # Banane (forte valeur ajoutee)

# Poids du scoring
W_USAGE = 0.35
W_SATELLITE = 0.25
W_TERRAIN = 0.20
W_TAILLE = 0.10
W_BIO = 0.10

# Constantes agronomiques
DENSITE_COCOTIER_HA = 143       # arbres/ha (espacement 8x8m)
NOIX_PAR_ARBRE_AN = 70          # noix/arbre/an (maturite)
PRIX_NOIX_EUR = 0.75            # EUR/noix (DAAF 2020)
POIDS_NOIX_KG = 1.2             # kg/noix
TAUX_PLANTATION_REALISTE = 0.15  # 15% de la surface brute
DELAI_MATURITE_ANS = 6

# Scores de pente (ZDH PRORATA)
SLOPE_SCORES = {
    '<10%': 1.0,
    '10-30%': 0.7,
    '30-50%': 0.3,
    '50-80%': 0.1,
    '>80%': 0.0,
}

# Communes de Guadeloupe
COMMUNES = {
    '97101': 'Les Abymes', '97102': 'Anse-Bertrand', '97103': 'Baie-Mahault',
    '97104': 'Baillif', '97105': 'Basse-Terre', '97106': 'Bouillante',
    '97107': 'Capesterre-Belle-Eau', '97108': 'Capesterre-de-Marie-Galante',
    '97109': 'Gourbeyre', '97110': 'La Desirade', '97111': 'Deshaies',
    '97112': 'Grand-Bourg', '97113': 'Le Gosier', '97114': 'Goyave',
    '97115': 'Lamentin', '97116': "Morne-a-l'Eau", '97117': 'Le Moule',
    '97118': 'Petit-Bourg', '97119': 'Petit-Canal', '97120': 'Pointe-a-Pitre',
    '97121': 'Pointe-Noire', '97122': 'Port-Louis', '97123': 'Saint-Barthelemy',
    '97124': 'Saint-Claude', '97125': 'Saint-Francois', '97126': 'Saint-Louis',
    '97127': 'Saint-Martin', '97128': 'Sainte-Anne', '97129': 'Sainte-Rose',
    '97130': 'Terre-de-Bas', '97131': 'Terre-de-Haut', '97132': 'Trois-Rivieres',
    '97133': 'Vieux-Fort', '97134': 'Vieux-Habitants',
}


# ============================================================================
# FONCTIONS
# ============================================================================

def load_rpg_data():
    """Charge les fichiers RPG GPKG et reprojette en WGS84."""
    print("Chargement des donnees RPG...")

    # Parcelles
    parcelles = gpd.read_file(RPG_DIR / 'RPG_Parcelles.gpkg')
    print(f"  Parcelles: {len(parcelles)} ({parcelles.crs})")

    # Calculer surface en ha dans le CRS UTM natif (metrique)
    parcelles['surface_ha'] = parcelles.geometry.area / 10000

    # Reprojeter en WGS84
    parcelles = parcelles.to_crs(TARGET_CRS)

    # Extraire code commune depuis id_parcel (format: 971_XXXXXXXX)
    parcelles['code_commune'] = parcelles['id_parcel'].str[:3]
    # Essayer d'extraire le code INSEE 5 chiffres si possible
    # Le format est "971_XXXXXXXX", pas de code commune direct
    # On utilisera une jointure spatiale ou le prefixe 971

    # ZDH (zones de difficulte)
    zdh = gpd.read_file(RPG_DIR / 'RPG_ZDH.gpkg')
    print(f"  ZDH: {len(zdh)} zones ({zdh.crs})")
    # Filtrer les geometries nulles
    zdh = zdh[zdh.geometry.notna()].to_crs(TARGET_CRS)

    # BIO
    bio = gpd.read_file(RPG_DIR / 'RPG_BIO.gpkg')
    print(f"  BIO: {len(bio)} parcelles ({bio.crs})")
    bio = bio.to_crs(TARGET_CRS)

    return parcelles, zdh, bio


def extract_satellite_stats(parcelles):
    """Extrait NDVI/EVI moyen par parcelle depuis le GeoTIFF local."""
    print("Extraction des statistiques satellite...")

    if not GEOTIFF_PATH.exists():
        print("  ATTENTION: GeoTIFF non trouve, scores satellite desactives")
        parcelles['ndvi_mean'] = np.nan
        parcelles['evi_mean'] = np.nan
        parcelles['pct_cocotier_sat'] = 0.0
        return parcelles

    raster_path = str(GEOTIFF_PATH)

    # NDVI moyen par parcelle
    print("  Extraction NDVI (bande 5)...")
    ndvi_results = zonal_stats(
        parcelles.geometry, raster_path, band=BAND_NDVI,
        stats=['mean'], nodata=0, all_touched=True
    )
    parcelles['ndvi_mean'] = [r['mean'] if r['mean'] is not None else np.nan for r in ndvi_results]

    # EVI moyen par parcelle
    print("  Extraction EVI (bande 6)...")
    evi_results = zonal_stats(
        parcelles.geometry, raster_path, band=BAND_EVI,
        stats=['mean'], nodata=0, all_touched=True
    )
    parcelles['evi_mean'] = [r['mean'] if r['mean'] is not None else np.nan for r in evi_results]

    # Pourcentage de pixels dans la plage cocotier
    # On utilise une approche simplifiee basee sur les moyennes
    ndvi = parcelles['ndvi_mean'].fillna(0)
    evi = parcelles['evi_mean'].fillna(0)
    parcelles['pct_cocotier_sat'] = np.where(
        (ndvi >= 0.40) & (ndvi <= 0.55) & (evi >= 0.25) & (evi <= 0.70),
        100.0,
        np.where(
            (ndvi >= 0.30) & (ndvi <= 0.60) & (evi >= 0.20) & (evi <= 0.75),
            50.0,
            0.0
        )
    )

    valid = parcelles['ndvi_mean'].notna().sum()
    print(f"  {valid}/{len(parcelles)} parcelles avec donnees satellite")
    return parcelles


def join_zdh_slope(parcelles, zdh):
    """Attribue un score de pente via jointure spatiale avec ZDH."""
    print("Jointure spatiale ZDH (pente)...")

    if len(zdh) == 0:
        parcelles['slope_score'] = 0.8
        parcelles['slope_category'] = None
        return parcelles

    # Colonnes ZDH
    zdh_cols = [c for c in zdh.columns if c.lower() == 'prorata'] + ['geometry']
    prorata_col = [c for c in zdh.columns if c.lower() == 'prorata']
    if not prorata_col:
        print("  Colonne PRORATA non trouvee, score par defaut 0.8")
        parcelles['slope_score'] = 0.8
        parcelles['slope_category'] = None
        return parcelles

    prorata_col = prorata_col[0]
    joined = gpd.sjoin(parcelles, zdh[[prorata_col, 'geometry']], how='left', predicate='intersects')

    # Deduplicate (garder premier match)
    joined = joined[~joined.index.duplicated(keep='first')]

    parcelles['slope_category'] = joined[prorata_col].values
    parcelles['slope_score'] = parcelles['slope_category'].map(SLOPE_SCORES).fillna(0.8)

    with_slope = parcelles['slope_category'].notna().sum()
    print(f"  {with_slope}/{len(parcelles)} parcelles avec donnees de pente")
    return parcelles


def join_bio(parcelles, bio):
    """Marque les parcelles en agriculture biologique via jointure spatiale."""
    print("Jointure parcelles bio...")

    if len(bio) == 0:
        parcelles['is_bio'] = False
        print("  0 parcelles bio (fichier vide)")
        return parcelles

    # Jointure spatiale : une parcelle est bio si elle intersecte une geometrie BIO
    bio_simple = bio[['geometry']].copy()
    bio_simple['_bio'] = True

    joined = gpd.sjoin(parcelles, bio_simple, how='left', predicate='intersects')
    joined = joined[~joined.index.duplicated(keep='first')]
    parcelles['is_bio'] = joined['_bio'].fillna(False).astype(bool).values

    nb_bio = parcelles['is_bio'].sum()
    print(f"  {nb_bio} parcelles bio")
    return parcelles


def compute_suitability_score(parcelles):
    """Calcule un score de potentiel cocotier (0-100) pour chaque parcelle."""
    print("Calcul des scores de potentiel...")

    # 1. Score usage actuel (0-100)
    usage_map = {}
    for code in CODES_EXISTANT_COCOTIER:
        usage_map[code] = -1  # Deja cocotier
    for code in CODES_HAUTE_DISPO:
        usage_map[code] = 100
    for code in CODES_MOYENNE_DISPO:
        usage_map[code] = 60
    for code in CODES_NON_CONVERTIBLE:
        usage_map[code] = 10

    parcelles['score_usage'] = parcelles['code_cultu'].map(usage_map).fillna(30)

    # 2. Score satellite (0-100)
    ndvi = parcelles['ndvi_mean'].fillna(0)
    evi = parcelles['evi_mean'].fillna(0)

    ndvi_score = np.where(
        (ndvi >= 0.40) & (ndvi <= 0.55), 100,
        np.where((ndvi >= 0.30) & (ndvi < 0.40), 60,
        np.where((ndvi > 0.55) & (ndvi <= 0.65), 50,
        np.where(ndvi < 0.15, 0, 30)))
    )
    evi_score = np.where(
        (evi >= 0.25) & (evi <= 0.70), 100,
        np.where((evi >= 0.15) & (evi < 0.25), 50,
        np.where(evi > 0.70, 30, 20))
    )
    parcelles['score_satellite'] = (ndvi_score * 0.6 + evi_score * 0.4).astype(float)

    # Si pas de donnees satellite, neutraliser le poids
    no_sat = parcelles['ndvi_mean'].isna()
    parcelles.loc[no_sat, 'score_satellite'] = 50  # Score neutre

    # 3. Score terrain (0-100)
    parcelles['score_terrain'] = (parcelles['slope_score'] * 100).fillna(80)

    # 4. Score taille (0-100)
    surface = parcelles['surface_ha']
    parcelles['score_taille'] = np.where(
        surface >= 5.0, 100,
        np.where(surface >= 2.0, 80,
        np.where(surface >= 0.5, 50,
        np.where(surface >= 0.1, 30, 10)))
    ).astype(float)

    # 5. Score bio (0 ou 100)
    parcelles['score_bio'] = np.where(parcelles['is_bio'], 100, 0).astype(float)

    # Score composite
    parcelles['score_potentiel'] = (
        parcelles['score_usage'] * W_USAGE +
        parcelles['score_satellite'] * W_SATELLITE +
        parcelles['score_terrain'] * W_TERRAIN +
        parcelles['score_taille'] * W_TAILLE +
        parcelles['score_bio'] * W_BIO
    ).round(1)

    # Marquer les cocotiers existants
    is_coco = parcelles['code_cultu'].isin(CODES_EXISTANT_COCOTIER)
    parcelles.loc[is_coco, 'score_potentiel'] = -1

    # Categoriser
    parcelles['categorie'] = np.where(
        parcelles['score_potentiel'] == -1, 'Cocotier existant',
        np.where(parcelles['score_potentiel'] >= 70, 'Potentiel eleve',
        np.where(parcelles['score_potentiel'] >= 45, 'Potentiel moyen',
        np.where(parcelles['score_potentiel'] >= 20, 'Potentiel faible',
        'Non adapte')))
    )

    # Afficher stats
    for cat in ['Cocotier existant', 'Potentiel eleve', 'Potentiel moyen', 'Potentiel faible', 'Non adapte']:
        mask = parcelles['categorie'] == cat
        print(f"  {cat}: {mask.sum()} parcelles, {parcelles.loc[mask, 'surface_ha'].sum():.1f} ha")

    return parcelles


def compute_volume_estimates(parcelles):
    """Calcule les estimations de volume pour les parcelles a potentiel."""
    print("Calcul des estimations de volume...")

    results = {}

    # Cocotier existant
    existant = parcelles[parcelles['categorie'] == 'Cocotier existant']
    results['existant'] = {
        'parcelles': int(len(existant)),
        'surface_ha': round(float(existant['surface_ha'].sum()), 1),
        'arbres_estimes': int(existant['surface_ha'].sum() * DENSITE_COCOTIER_HA),
        'production_noix_an': int(existant['surface_ha'].sum() * DENSITE_COCOTIER_HA * NOIX_PAR_ARBRE_AN),
        'badge': 'Reel',
    }

    # Par categorie de potentiel
    for cat, label in [('Potentiel eleve', 'potentiel_eleve'),
                       ('Potentiel moyen', 'potentiel_moyen'),
                       ('Potentiel faible', 'potentiel_faible')]:
        subset = parcelles[parcelles['categorie'] == cat]
        surface_brute = float(subset['surface_ha'].sum())
        surface_realiste = surface_brute * TAUX_PLANTATION_REALISTE

        results[label] = {
            'parcelles': int(len(subset)),
            'surface_brute_ha': round(surface_brute, 1),
            'surface_realiste_ha': round(surface_realiste, 1),
            'arbres_potentiels': int(surface_realiste * DENSITE_COCOTIER_HA),
            'production_noix_an': int(surface_realiste * DENSITE_COCOTIER_HA * NOIX_PAR_ARBRE_AN),
            'production_tonnes_an': round(surface_realiste * DENSITE_COCOTIER_HA * NOIX_PAR_ARBRE_AN * POIDS_NOIX_KG / 1000, 1),
            'valeur_eur_an': round(surface_realiste * DENSITE_COCOTIER_HA * NOIX_PAR_ARBRE_AN * PRIX_NOIX_EUR, 0),
            'badge': 'Calcule' if cat == 'Potentiel eleve' else 'Estime',
        }

    # Total disponible (eleve + moyen + faible)
    total_brute = sum(results[k].get('surface_brute_ha', 0) for k in ['potentiel_eleve', 'potentiel_moyen', 'potentiel_faible'])
    total_realiste = sum(results[k].get('surface_realiste_ha', 0) for k in ['potentiel_eleve', 'potentiel_moyen', 'potentiel_faible'])

    results['total_disponible'] = {
        'surface_brute_ha': round(total_brute, 1),
        'surface_realiste_ha': round(total_realiste, 1),
        'arbres_potentiels': int(total_realiste * DENSITE_COCOTIER_HA),
        'production_noix_an': int(total_realiste * DENSITE_COCOTIER_HA * NOIX_PAR_ARBRE_AN),
        'production_tonnes_an': round(total_realiste * DENSITE_COCOTIER_HA * NOIX_PAR_ARBRE_AN * POIDS_NOIX_KG / 1000, 1),
        'valeur_eur_an': round(total_realiste * DENSITE_COCOTIER_HA * NOIX_PAR_ARBRE_AN * PRIX_NOIX_EUR, 0),
        'taux_plantation': TAUX_PLANTATION_REALISTE,
        'delai_maturite_ans': DELAI_MATURITE_ANS,
        'badge': 'Estime',
    }

    # Non adapte
    non_adapte = parcelles[parcelles['categorie'] == 'Non adapte']
    results['non_adapte'] = {
        'parcelles': int(len(non_adapte)),
        'surface_ha': round(float(non_adapte['surface_ha'].sum()), 1),
    }

    return results


def compute_stats_par_commune(parcelles):
    """Statistiques par commune via jointure spatiale avec limites communales."""
    print("Calcul des statistiques par commune...")

    # Les id_parcel commencent par "971_" sans code commune direct
    # On utilise une approche spatiale : centroide de chaque parcelle
    # Pour simplifier, on regroupe par les coordonnees (latitude)
    # Approximation : on attribue les communes connues du cadastre IGN

    # Approche simplifiee : on groupe toutes les parcelles ensemble
    # et on identifie les zones par cluster geographique
    dispo = parcelles[parcelles['categorie'].isin(['Potentiel eleve', 'Potentiel moyen'])]
    existant = parcelles[parcelles['categorie'] == 'Cocotier existant']

    # Statistiques globales par categorie (pas par commune sans jointure admin)
    # On fait une grille lon/lat pour creer des zones
    parcelles['centroid_lon'] = parcelles.geometry.centroid.x
    parcelles['centroid_lat'] = parcelles.geometry.centroid.y

    # Decoupage en zones geographiques simples
    zones = []
    zone_defs = [
        ('Grande-Terre Nord', -61.35, 16.30, -61.00, 16.52),
        ('Grande-Terre Centre', -61.55, 16.15, -61.10, 16.35),
        ('Grande-Terre Sud', -61.45, 16.00, -61.10, 16.20),
        ('Basse-Terre Nord', -61.81, 16.15, -61.50, 16.52),
        ('Basse-Terre Centre', -61.81, 16.00, -61.55, 16.20),
        ('Basse-Terre Sud', -61.81, 15.83, -61.55, 16.05),
        ('Marie-Galante', -61.35, 15.83, -61.15, 16.05),
        ('Les Saintes', -61.70, 15.83, -61.50, 15.95),
        ('La Desirade', -61.10, 16.28, -60.99, 16.40),
    ]

    for nom, lon_min, lat_min, lon_max, lat_max in zone_defs:
        mask = (
            (parcelles['centroid_lon'] >= lon_min) & (parcelles['centroid_lon'] <= lon_max) &
            (parcelles['centroid_lat'] >= lat_min) & (parcelles['centroid_lat'] <= lat_max)
        )
        sub = parcelles[mask]
        if len(sub) == 0:
            continue

        dispo_z = sub[sub['categorie'].isin(['Potentiel eleve', 'Potentiel moyen'])]
        exist_z = sub[sub['categorie'] == 'Cocotier existant']
        eleve_z = sub[sub['categorie'] == 'Potentiel eleve']

        zones.append({
            'nom': nom,
            'total_parcelles': int(len(sub)),
            'parcelles_cocotier_existant': int(len(exist_z)),
            'surface_cocotier_ha': round(float(exist_z['surface_ha'].sum()), 1),
            'parcelles_potentiel': int(len(dispo_z)),
            'surface_dispo_ha': round(float(dispo_z['surface_ha'].sum()), 1),
            'parcelles_potentiel_eleve': int(len(eleve_z)),
            'surface_potentiel_eleve_ha': round(float(eleve_z['surface_ha'].sum()), 1),
            'score_moyen': round(float(sub.loc[sub['score_potentiel'] >= 0, 'score_potentiel'].mean()), 1) if (sub['score_potentiel'] >= 0).any() else 0,
        })

    zones.sort(key=lambda x: x['surface_dispo_ha'], reverse=True)
    return zones


def create_parcelles_map(parcelles):
    """Genere une carte Folium avec les parcelles colorees par potentiel."""
    print("Generation de la carte des parcelles...")

    center = [16.18, -61.55]
    m = folium.Map(location=center, zoom_start=10, tiles='OpenStreetMap')

    # Fond satellite
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri', name='Satellite', overlay=False
    ).add_to(m)

    # Couleurs par categorie
    colors = {
        'Cocotier existant': '#2196F3',
        'Potentiel eleve': '#4CAF50',
        'Potentiel moyen': '#FFC107',
        'Potentiel faible': '#FF9800',
        'Non adapte': '#9E9E9E',
    }

    # Ajouter chaque categorie comme FeatureGroup
    for categorie, color in colors.items():
        subset = parcelles[parcelles['categorie'] == categorie].copy()
        if len(subset) == 0:
            continue

        # Simplifier les geometries pour performance
        subset['geometry'] = subset.geometry.simplify(0.0001)

        # Pour "Non adapte", ne pas afficher par defaut (trop de parcelles)
        show = categorie != 'Non adapte'

        fg = folium.FeatureGroup(
            name=f"{categorie} ({len(subset)})",
            show=show
        )

        geojson_data = json.loads(subset[['geometry', 'id_parcel', 'code_cultu',
                                           'surface_ha', 'score_potentiel', 'categorie']].to_json())

        folium.GeoJson(
            geojson_data,
            style_function=lambda feature, c=color: {
                'fillColor': c,
                'color': c,
                'weight': 1,
                'fillOpacity': 0.5,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=['id_parcel', 'code_cultu', 'surface_ha', 'score_potentiel', 'categorie'],
                aliases=['Parcelle:', 'Culture:', 'Surface (ha):', 'Score:', 'Categorie:'],
                localize=True,
            ),
        ).add_to(fg)

        fg.add_to(m)

    # Legende
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                background:white;padding:12px 16px;border-radius:8px;
                box-shadow:0 2px 6px rgba(0,0,0,0.3);font-size:13px;line-height:1.8;">
        <b style="font-size:14px;">Potentiel cocotier</b><br>
        <span style="color:#2196F3;font-size:16px;">&#9632;</span> Cocotier existant<br>
        <span style="color:#4CAF50;font-size:16px;">&#9632;</span> Potentiel eleve (&ge;70)<br>
        <span style="color:#FFC107;font-size:16px;">&#9632;</span> Potentiel moyen (45-69)<br>
        <span style="color:#FF9800;font-size:16px;">&#9632;</span> Potentiel faible (20-44)<br>
        <span style="color:#9E9E9E;font-size:16px;">&#9632;</span> Non adapte (&lt;20)
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    folium.LayerControl().add_to(m)

    output_file = OUTPUT_DIR / 'carte_parcelles.html'
    m.save(str(output_file))
    print(f"  Carte: {output_file}")
    return output_file


def save_results(parcelles, volumes, zones):
    """Sauvegarde les resultats en JSON."""
    print("Sauvegarde des resultats...")

    # Top 100 parcelles par score
    top = parcelles[parcelles['score_potentiel'] > 0].nlargest(100, 'score_potentiel')
    top_list = []
    for _, row in top.iterrows():
        top_list.append({
            'id_parcel': str(row['id_parcel']),
            'code_culture': str(row['code_cultu']),
            'surface_ha': round(float(row['surface_ha']), 2),
            'score_potentiel': round(float(row['score_potentiel']), 1),
            'categorie': str(row['categorie']),
            'ndvi_mean': round(float(row['ndvi_mean']), 3) if not np.isnan(row['ndvi_mean']) else None,
            'evi_mean': round(float(row['evi_mean']), 3) if not np.isnan(row['evi_mean']) else None,
            'slope_category': str(row.get('slope_category', '')) if row.get('slope_category') is not None else None,
            'is_bio': bool(row.get('is_bio', False)),
        })

    # Synthese par code culture
    code_summary = []
    for code, group in parcelles.groupby('code_cultu'):
        valid_scores = group.loc[group['score_potentiel'] >= 0, 'score_potentiel']
        code_summary.append({
            'code': str(code),
            'parcelles': int(len(group)),
            'surface_ha': round(float(group['surface_ha'].sum()), 1),
            'score_moyen': round(float(valid_scores.mean()), 1) if len(valid_scores) > 0 else 0,
        })
    code_summary.sort(key=lambda x: x['surface_ha'], reverse=True)

    analysis = {
        'date_analyse': datetime.now().isoformat(),
        'source': 'RPG GPKG 2024 + Sentinel-2 classification',
        'total_parcelles_rpg': int(len(parcelles)),
        'surface_totale_rpg_ha': round(float(parcelles['surface_ha'].sum()), 1),
        'parametres': {
            'densite_cocotier_ha': DENSITE_COCOTIER_HA,
            'noix_par_arbre_an': NOIX_PAR_ARBRE_AN,
            'prix_noix_eur': PRIX_NOIX_EUR,
            'poids_noix_kg': POIDS_NOIX_KG,
            'taux_plantation_realiste': TAUX_PLANTATION_REALISTE,
            'delai_maturite_ans': DELAI_MATURITE_ANS,
        },
        'poids_scoring': {
            'usage_actuel': W_USAGE,
            'satellite': W_SATELLITE,
            'terrain': W_TERRAIN,
            'taille': W_TAILLE,
            'bio': W_BIO,
        },
        'volumes': volumes,
        'par_zone': zones,
        'par_code_culture': code_summary[:25],
        'top_parcelles': top_list,
    }

    output_file = OUTPUT_DIR / 'parcelles_analysis.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    print(f"  Resultats: {output_file}")
    return analysis


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("ANALYSE PARCELLAIRE - POTENTIEL COCOTIER")
    print("Coconut Mapping - Guadeloupe")
    print("=" * 60)
    print()

    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Charger les donnees RPG
    parcelles, zdh, bio = load_rpg_data()

    # 2. Extraire stats satellite
    parcelles = extract_satellite_stats(parcelles)

    # 3. Jointure pente ZDH
    parcelles = join_zdh_slope(parcelles, zdh)

    # 4. Jointure bio
    parcelles = join_bio(parcelles, bio)

    # 5. Scoring
    parcelles = compute_suitability_score(parcelles)

    # 6. Volumes
    volumes = compute_volume_estimates(parcelles)

    # 7. Stats par zone
    zones = compute_stats_par_commune(parcelles)

    # 8. Carte
    create_parcelles_map(parcelles)

    # 9. Sauvegarder
    analysis = save_results(parcelles, volumes, zones)

    print()
    print("=" * 60)
    print("ANALYSE TERMINEE")
    print("=" * 60)
    v = volumes
    print(f"  Parcelles analysees   : {len(parcelles)}")
    print(f"  Cocotier existant     : {v['existant']['parcelles']} parcelles, {v['existant']['surface_ha']} ha")
    print(f"  Potentiel eleve       : {v['potentiel_eleve']['parcelles']} parcelles, {v['potentiel_eleve']['surface_brute_ha']} ha brut")
    print(f"  Potentiel moyen       : {v['potentiel_moyen']['parcelles']} parcelles, {v['potentiel_moyen']['surface_brute_ha']} ha brut")
    print(f"  Surface plantable     : {v['total_disponible']['surface_realiste_ha']} ha (realiste, 15%)")
    print(f"  Arbres potentiels     : {v['total_disponible']['arbres_potentiels']:,}")
    print(f"  Production potentielle: {v['total_disponible']['production_noix_an']:,} noix/an")
    print(f"  Valeur potentielle    : {v['total_disponible']['valeur_eur_an']:,.0f} EUR/an")
    print()


if __name__ == '__main__':
    main()
