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
import time
import urllib.request
import urllib.parse
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
W_PLU = 0.10
W_TAILLE = 0.10

# URL WFS GPU pour les zonages PLU
GPU_WFS_URL = 'https://data.geopf.fr/wfs/ows'

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

    # Jointure spatiale communes (contours geo.api.gouv.fr)
    communes_file = BASE_DIR / 'communes_guadeloupe.geojson'
    if communes_file.exists():
        print("  Jointure spatiale communes...")
        communes = gpd.read_file(communes_file).to_crs(TARGET_CRS)
        communes = communes.rename(columns={'nom': 'commune', 'code': 'code_commune'})
        # Jointure par centroide pour eviter les doublons multi-intersection
        centroids = parcelles.copy()
        centroids['geometry'] = centroids.geometry.centroid
        joined = gpd.sjoin(centroids[['geometry']], communes[['commune', 'code_commune', 'geometry']], how='left', predicate='within')
        joined = joined[~joined.index.duplicated(keep='first')]
        parcelles['commune'] = joined['commune'].values
        parcelles['code_commune'] = joined['code_commune'].values
        with_commune = parcelles['commune'].notna().sum()
        print(f"  {with_commune}/{len(parcelles)} parcelles avec commune identifiee")
    else:
        print("  ATTENTION: communes_guadeloupe.geojson non trouve, communes non disponibles")
        parcelles['commune'] = None
        parcelles['code_commune'] = None

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


def fetch_plu_zones():
    """Telecharge les zones agricoles (type A) du PLU via le WFS GPU."""
    print("Telechargement des zones PLU (agricoles)...")

    plu_cache = OUTPUT_DIR / 'plu_zones_agricoles.geojson'
    if plu_cache.exists():
        print("  Cache local trouve, chargement...")
        zones = gpd.read_file(plu_cache)
        print(f"  {len(zones)} zones agricoles chargees depuis le cache")
        return zones

    # Requete WFS pour toutes les zones agricoles de Guadeloupe
    params = urllib.parse.urlencode({
        'SERVICE': 'WFS',
        'VERSION': '2.0.0',
        'REQUEST': 'GetFeature',
        'TYPENAMES': 'wfs_du:zone_urba',
        'CQL_FILTER': "partition LIKE 'DU_971%' AND typezone='A'",
        'OUTPUTFORMAT': 'application/json',
    })
    url = f'{GPU_WFS_URL}?{params}'

    try:
        print("  Requete WFS GPU...")
        req = urllib.request.Request(url, headers={'User-Agent': 'CoconutMapping/1.0'})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())

        zones = gpd.GeoDataFrame.from_features(data['features'], crs='EPSG:4326')
        print(f"  {len(zones)} zones agricoles telechargees")

        # Sauvegarder en cache
        zones.to_file(plu_cache, driver='GeoJSON')
        print(f"  Cache sauvegarde: {plu_cache}")
        return zones

    except Exception as e:
        print(f"  ERREUR telechargement PLU: {e}")
        print("  Score PLU desactive (toutes les parcelles = neutre)")
        return gpd.GeoDataFrame(columns=['geometry', 'typezone'], crs='EPSG:4326')


def join_plu(parcelles, plu_zones):
    """Marque les parcelles situees en zone agricole du PLU."""
    print("Jointure spatiale PLU...")

    if len(plu_zones) == 0:
        parcelles['in_zone_agricole'] = True  # Par defaut si pas de donnee
        print("  Pas de donnees PLU, toutes les parcelles considerees en zone agricole")
        return parcelles

    # Jointure spatiale : une parcelle est en zone A si son centroide est dans une zone A
    centroids = parcelles.copy()
    centroids['geometry'] = centroids.geometry.centroid

    plu_simple = plu_zones[['geometry']].copy()
    plu_simple['_in_zone_a'] = True

    joined = gpd.sjoin(centroids[['geometry']], plu_simple, how='left', predicate='within')
    joined = joined[~joined.index.duplicated(keep='first')]

    parcelles['in_zone_agricole'] = joined['_in_zone_a'].fillna(False).astype(bool).values

    # Pour les communes sans PLU (7 communes), considerer les parcelles RPG comme en zone agricole
    # On identifie les communes sans PLU par l'absence de zones A
    communes_avec_plu = set()
    if 'partition' in plu_zones.columns:
        for p in plu_zones['partition'].dropna().unique():
            # partition = 'DU_97105' -> code = '97105'
            code = p.replace('DU_', '')
            communes_avec_plu.add(code)

    if communes_avec_plu and 'code_commune' in parcelles.columns:
        sans_plu = ~parcelles['code_commune'].isin(communes_avec_plu)
        parcelles.loc[sans_plu, 'in_zone_agricole'] = True
        nb_sans_plu = sans_plu.sum()
        if nb_sans_plu > 0:
            print(f"  {nb_sans_plu} parcelles dans communes sans PLU (considerees en zone agricole)")

    nb_zone_a = parcelles['in_zone_agricole'].sum()
    nb_hors = (~parcelles['in_zone_agricole']).sum()
    print(f"  {nb_zone_a} parcelles en zone agricole PLU")
    print(f"  {nb_hors} parcelles hors zone agricole (penalisees)")
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

    # 5. Score PLU (0 ou 100) - zone agricole du PLU
    parcelles['score_plu'] = np.where(parcelles['in_zone_agricole'], 100, 0).astype(float)

    # Score composite
    parcelles['score_potentiel'] = (
        parcelles['score_usage'] * W_USAGE +
        parcelles['score_satellite'] * W_SATELLITE +
        parcelles['score_terrain'] * W_TERRAIN +
        parcelles['score_plu'] * W_PLU +
        parcelles['score_taille'] * W_TAILLE
    ).round(1)

    # Marquer les cocotiers existants
    is_coco = parcelles['code_cultu'].isin(CODES_EXISTANT_COCOTIER)
    parcelles.loc[is_coco, 'score_potentiel'] = -1

    # Categoriser
    parcelles['categorie'] = np.where(
        parcelles['score_potentiel'] == -1, 'Cocotiers existants',
        np.where(parcelles['score_potentiel'] >= 70, 'Potentiel eleve',
        np.where(parcelles['score_potentiel'] >= 45, 'Potentiel moyen',
        np.where(parcelles['score_potentiel'] >= 20, 'Potentiel faible',
        'Non adapte')))
    )

    # Afficher stats
    for cat in ['Cocotiers existants', 'Potentiel eleve', 'Potentiel moyen', 'Potentiel faible', 'Non adapte']:
        mask = parcelles['categorie'] == cat
        print(f"  {cat}: {mask.sum()} parcelles, {parcelles.loc[mask, 'surface_ha'].sum():.1f} ha")

    return parcelles


def compute_volume_estimates(parcelles):
    """Calcule les estimations de volume pour les parcelles a potentiel."""
    print("Calcul des estimations de volume...")

    results = {}

    # Cocotiers existants
    existant = parcelles[parcelles['categorie'] == 'Cocotiers existants']
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
    existant = parcelles[parcelles['categorie'] == 'Cocotiers existants']

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
        exist_z = sub[sub['categorie'] == 'Cocotiers existants']
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
        'Cocotiers existants': '#2196F3',
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
        <span style="color:#2196F3;font-size:16px;">&#9632;</span> Cocotiers existants<br>
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


def reverse_geocode(lat, lon):
    """Reverse geocode via Nominatim (OpenStreetMap). Retourne le lieu-dit ou None."""
    try:
        url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&zoom=16&addressdetails=1'
        req = urllib.request.Request(url, headers={'User-Agent': 'CoconutMapping/1.0'})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())
        addr = data.get('address', {})
        # Construire le lieu-dit : road > hamlet > locality > suburb
        road = addr.get('road', '')
        hamlet = addr.get('hamlet', addr.get('locality', addr.get('suburb', '')))
        if road and hamlet:
            return f"{hamlet}, {road}"
        return road or hamlet or None
    except Exception:
        return None


def save_results(parcelles, volumes, zones):
    """Sauvegarde les resultats en JSON."""
    print("Sauvegarde des resultats...")

    # Top 100 parcelles par score
    top = parcelles[parcelles['score_potentiel'] > 0].nlargest(100, 'score_potentiel')

    # Calcul des centroides GPS
    print("  Calcul des coordonnees GPS...")
    top_centroids = top.geometry.centroid

    # Reverse geocoding Nominatim pour les lieux-dits
    print("  Reverse geocoding (Nominatim)... ~100 requetes, 1/s")
    lieux_dits = {}
    for i, (idx, row) in enumerate(top.iterrows()):
        c = top_centroids.loc[idx]
        lat, lon = c.y, c.x
        lieu = reverse_geocode(lat, lon)
        lieux_dits[idx] = lieu
        if (i + 1) % 10 == 0:
            print(f"    {i + 1}/100 parcelles geocodees...")
        time.sleep(1.1)  # Respecter le rate limit Nominatim

    found = sum(1 for v in lieux_dits.values() if v)
    print(f"  {found}/100 lieux-dits trouves")

    top_list = []
    for _, row in top.iterrows():
        commune_val = row.get('commune')
        centroid = top_centroids.loc[row.name]
        lat, lon = round(centroid.y, 6), round(centroid.x, 6)
        top_list.append({
            'id_parcel': str(row['id_parcel']),
            'commune': str(commune_val) if commune_val is not None and str(commune_val) != 'nan' else None,
            'lieu_dit': lieux_dits.get(row.name),
            'lat': lat,
            'lon': lon,
            'code_culture': str(row['code_cultu']),
            'surface_ha': round(float(row['surface_ha']), 2),
            'score_potentiel': round(float(row['score_potentiel']), 1),
            'categorie': str(row['categorie']),
            'ndvi_mean': round(float(row['ndvi_mean']), 3) if not np.isnan(row['ndvi_mean']) else None,
            'evi_mean': round(float(row['evi_mean']), 3) if not np.isnan(row['evi_mean']) else None,
            'slope_category': str(row.get('slope_category', '')) if row.get('slope_category') is not None else None,
            'in_zone_agricole': bool(row.get('in_zone_agricole', True)),
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
            'plu': W_PLU,
            'taille': W_TAILLE,
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


def create_top100_map(parcelles):
    """Genere une carte Folium avec uniquement les top 100 parcelles."""
    print("Generation de la carte Top 100...")

    top = parcelles[parcelles['score_potentiel'] > 0].nlargest(100, 'score_potentiel').copy()
    top['geometry'] = top.geometry.simplify(0.0001)

    # Centrer la carte sur les parcelles
    centroids = top.geometry.centroid
    center_lat = centroids.y.mean()
    center_lon = centroids.x.mean()

    m = folium.Map(location=[center_lat, center_lon], zoom_start=11, tiles='OpenStreetMap')

    # Fond satellite
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri', name='Satellite', overlay=False
    ).add_to(m)

    # Couleur par score
    def score_color(score):
        if score >= 90:
            return '#15803d'  # vert fonce
        elif score >= 80:
            return '#22c55e'  # vert
        elif score >= 70:
            return '#4ade80'  # vert clair
        elif score >= 60:
            return '#facc15'  # jaune
        else:
            return '#fb923c'  # orange

    # Ajouter les parcelles
    fg = folium.FeatureGroup(name=f"Top 100 parcelles")

    for rank, (idx, row) in enumerate(top.iterrows(), 1):
        color = score_color(row['score_potentiel'])
        centroid = row.geometry.centroid
        commune = row.get('commune', '') or ''
        plu_label = ' | Zone A' if row.get('in_zone_agricole') else ''

        popup_html = (
            f"<div style='font-family:sans-serif;font-size:12px;min-width:200px;'>"
            f"<b>#{rank}</b> - {row['id_parcel']}<br>"
            f"<b>{commune}</b><br>"
            f"Culture: {row['code_cultu']} | {row['surface_ha']:.2f} ha{plu_label}<br>"
            f"<b>Score: {row['score_potentiel']}</b><br>"
            f"NDVI: {row['ndvi_mean']:.3f} | EVI: {row['evi_mean']:.3f}<br>"
            f"<a href='https://www.google.com/maps?q={centroid.y},{centroid.x}&z=16' target='_blank'>Google Maps</a>"
            f"</div>"
        )

        # Polygone de la parcelle
        folium.GeoJson(
            row.geometry.__geo_interface__,
            style_function=lambda f, c=color: {
                'fillColor': c, 'color': '#ffffff', 'weight': 2,
                'fillOpacity': 0.7,
            },
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"#{rank} {row['id_parcel']} — Score {row['score_potentiel']} — {commune}",
        ).add_to(fg)

        # Marqueur numerote au centroide (avec popup identique)
        folium.CircleMarker(
            location=[centroid.y, centroid.x],
            radius=6,
            color='#ffffff',
            fill=True,
            fill_color=color,
            fill_opacity=1.0,
            weight=2,
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"#{rank}",
        ).add_to(fg)

    fg.add_to(m)

    folium.LayerControl().add_to(m)

    output_file = OUTPUT_DIR / 'carte_top100.html'
    m.save(str(output_file))
    print(f"  Carte Top 100: {output_file}")
    return output_file


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
    parcelles, zdh, _bio = load_rpg_data()

    # 2. Extraire stats satellite
    parcelles = extract_satellite_stats(parcelles)

    # 3. Jointure pente ZDH
    parcelles = join_zdh_slope(parcelles, zdh)

    # 4. Telechargement et jointure PLU (zones agricoles)
    plu_zones = fetch_plu_zones()
    parcelles = join_plu(parcelles, plu_zones)

    # 5. Scoring
    parcelles = compute_suitability_score(parcelles)

    # 6. Volumes
    volumes = compute_volume_estimates(parcelles)

    # 7. Stats par zone
    zones = compute_stats_par_commune(parcelles)

    # 8. Carte complete
    create_parcelles_map(parcelles)

    # 9. Carte top 100
    create_top100_map(parcelles)

    # 10. Sauvegarder (avec reverse geocoding)
    analysis = save_results(parcelles, volumes, zones)

    print()
    print("=" * 60)
    print("ANALYSE TERMINEE")
    print("=" * 60)
    v = volumes
    print(f"  Parcelles analysees   : {len(parcelles)}")
    print(f"  Cocotiers existants     : {v['existant']['parcelles']} parcelles, {v['existant']['surface_ha']} ha")
    print(f"  Potentiel eleve       : {v['potentiel_eleve']['parcelles']} parcelles, {v['potentiel_eleve']['surface_brute_ha']} ha brut")
    print(f"  Potentiel moyen       : {v['potentiel_moyen']['parcelles']} parcelles, {v['potentiel_moyen']['surface_brute_ha']} ha brut")
    print(f"  Surface plantable     : {v['total_disponible']['surface_realiste_ha']} ha (realiste, 15%)")
    print(f"  Arbres potentiels     : {v['total_disponible']['arbres_potentiels']:,}")
    print(f"  Production potentielle: {v['total_disponible']['production_noix_an']:,} noix/an")
    print(f"  Valeur potentielle    : {v['total_disponible']['valeur_eur_an']:,.0f} EUR/an")
    print()


if __name__ == '__main__':
    main()
