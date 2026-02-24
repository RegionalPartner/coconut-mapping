#!/usr/bin/env python3
"""
DETECTION DE ZONES DE COCOTIERS - Guadeloupe
Classification spectrale multi-indices via Google Earth Engine.

Utilise le composite Sentinel-2 2024 pour identifier les zones
a forte probabilite de cocotiers par analyse spectrale.
"""

import ee
import json
import folium
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

SERVICE_ACCOUNT_FILE = 'gen-lang-client-0363696684-80e30fa97142.json'
GUADELOUPE_BBOX = [-61.81, 15.83, -61.00, 16.52]
START_DATE = '2024-01-01'
END_DATE = '2024-12-31'
MAX_CLOUD_COVER = 20
OUTPUT_DIR = Path('output_imagery')

# Seuils de classification
THRESHOLDS = {
    'eau_ndwi': 0.3,
    'sol_nu_ndvi': 0.15,
    'urbain_ndvi': 0.10,
    'culture_ndvi_min': 0.25,
    'culture_ndvi_max': 0.45,
    'cocotier_ndvi_min': 0.40,
    'cocotier_evi_min': 0.25,
    'cocotier_evi_max': 0.70,
    'foret_ndvi': 0.55,
    'foret_evi': 0.45,
}

# Estimation cocotiers
DENSITE_ARBRES_HA = 120     # ~120 cocotiers/ha en plantation
NOIX_PAR_ARBRE_AN = 70      # ~70 noix/arbre/an
PRIX_NOIX_EUR = 0.25        # prix moyen noix de coco

# Couleurs de classification
COLORS = {
    'eau': '#2196F3',
    'sol_nu': '#795548',
    'urbain': '#9E9E9E',
    'cultures': '#FFC107',
    'cocotiers': '#4CAF50',
    'foret': '#1B5E20',
    'autre_vegetation': '#8BC34A',
}

# ============================================================================
# FONCTIONS EARTH ENGINE
# ============================================================================

def initialize_ee():
    """Initialise Earth Engine."""
    print("Initialisation Google Earth Engine...")
    with open(SERVICE_ACCOUNT_FILE, 'r') as f:
        sa_info = json.load(f)
    credentials = ee.ServiceAccountCredentials(
        email=sa_info['client_email'],
        key_file=SERVICE_ACCOUNT_FILE
    )
    ee.Initialize(credentials)
    print(f"  Connecte: {sa_info['client_email']}")


def mask_clouds(image):
    """Masque nuages et cirrus."""
    qa = image.select('QA60')
    mask = qa.bitwiseAnd(1 << 10).eq(0).And(qa.bitwiseAnd(1 << 11).eq(0))
    return image.updateMask(mask).divide(10000)


def get_composite(roi):
    """Charge le composite Sentinel-2."""
    print("Chargement composite Sentinel-2...")
    collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
        .filterBounds(roi)
        .filterDate(START_DATE, END_DATE)
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', MAX_CLOUD_COVER))
        .map(mask_clouds))

    count = collection.size().getInfo()
    print(f"  {count} images utilisees")
    return collection.median()


def compute_indices(composite):
    """Calcule les indices spectraux."""
    print("Calcul des indices spectraux...")

    ndvi = composite.normalizedDifference(['B8', 'B4']).rename('NDVI')
    evi = composite.expression(
        '2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))', {
            'NIR': composite.select('B8'),
            'RED': composite.select('B4'),
            'BLUE': composite.select('B2')
        }
    ).rename('EVI')
    ndwi = composite.normalizedDifference(['B3', 'B8']).rename('NDWI')
    nir_red_ratio = composite.select('B8').divide(
        composite.select('B4').add(0.001)
    ).rename('NIR_RED')

    indices = composite.addBands([ndvi, evi, ndwi, nir_red_ratio])

    # Stats
    stats = indices.select(['NDVI', 'EVI', 'NDWI']).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=ee.Geometry.Rectangle(GUADELOUPE_BBOX),
        scale=100,
        maxPixels=1e9
    ).getInfo()
    print(f"  NDVI moyen: {stats.get('NDVI', 0):.3f}")
    print(f"  EVI moyen:  {stats.get('EVI', 0):.3f}")
    print(f"  NDWI moyen: {stats.get('NDWI', 0):.3f}")

    return indices


def classify(indices):
    """Classification par regles spectrales."""
    print("Classification en cours...")
    t = THRESHOLDS

    ndvi = indices.select('NDVI')
    evi = indices.select('EVI')
    ndwi = indices.select('NDWI')

    # Classes (valeurs: 1=eau, 2=sol_nu, 3=urbain, 4=cultures, 5=cocotiers, 6=foret, 7=autre_veg)
    # On construit par priorite decroissante

    # Eau
    eau = ndwi.gt(t['eau_ndwi'])

    # Sol nu
    sol_nu = ndvi.lt(t['sol_nu_ndvi']).And(eau.Not())

    # Urbain
    urbain = ndvi.gte(t['urbain_ndvi']).And(ndvi.lt(t['sol_nu_ndvi'])).And(eau.Not())

    # Foret dense (NDVI et EVI eleves)
    foret = ndvi.gt(t['foret_ndvi']).And(evi.gt(t['foret_evi'])).And(eau.Not())

    # Cocotiers probables : NDVI moyen-haut, EVI dans une plage typique palmiers
    cocotiers = (ndvi.gte(t['cocotier_ndvi_min'])
        .And(ndvi.lte(t['foret_ndvi']))
        .And(evi.gte(t['cocotier_evi_min']))
        .And(evi.lte(t['cocotier_evi_max']))
        .And(eau.Not())
        .And(foret.Not()))

    # Cultures basses (canne, maraichage)
    cultures = (ndvi.gte(t['culture_ndvi_min'])
        .And(ndvi.lt(t['cocotier_ndvi_min']))
        .And(eau.Not())
        .And(sol_nu.Not()))

    # Autre vegetation
    autre_veg = (eau.Not()
        .And(sol_nu.Not())
        .And(urbain.Not())
        .And(foret.Not())
        .And(cocotiers.Not())
        .And(cultures.Not())
        .And(ndvi.gt(t['sol_nu_ndvi'])))

    # Image classifiee
    classified = (ee.Image(0)
        .where(eau, 1)
        .where(sol_nu, 2)
        .where(urbain, 3)
        .where(cultures, 4)
        .where(cocotiers, 5)
        .where(foret, 6)
        .where(autre_veg, 7)
        .rename('classification'))

    # Filtre morphologique pour reduire le bruit (mode focal 3x3)
    classified_clean = classified.focal_mode(
        radius=30, kernelType='circle', units='meters'
    ).rename('classification')

    print("  Classification terminee (7 classes)")
    return classified_clean


def compute_statistics(classified, roi):
    """Calcule les surfaces par classe."""
    print("Calcul des statistiques...")

    class_names = {
        1: 'Eau',
        2: 'Sol nu',
        3: 'Urbain',
        4: 'Cultures (canne, etc.)',
        5: 'Cocotiers probables',
        6: 'Foret dense',
        7: 'Autre vegetation',
    }

    # Surface par pixel = 10m x 10m = 100m2
    pixel_area = ee.Image.pixelArea()

    stats = {}
    for class_val, class_name in class_names.items():
        mask = classified.eq(class_val)
        area_m2 = pixel_area.updateMask(mask).reduceRegion(
            reducer=ee.Reducer.sum(),
            geometry=roi,
            scale=30,  # echelle de reduction pour performance
            maxPixels=1e10
        ).getInfo()

        area_val = area_m2.get('area', 0) or 0
        area_ha = area_val / 10000
        area_km2 = area_val / 1e6

        stats[class_name] = {
            'hectares': round(area_ha, 1),
            'km2': round(area_km2, 2),
        }
        print(f"  {class_name}: {area_ha:.0f} ha ({area_km2:.1f} km2)")

    # Estimations cocotiers
    coco_ha = stats['Cocotiers probables']['hectares']
    nb_arbres = int(coco_ha * DENSITE_ARBRES_HA)
    production_noix = nb_arbres * NOIX_PAR_ARBRE_AN
    valeur_eur = production_noix * PRIX_NOIX_EUR

    estimations = {
        'surface_cocotiers_ha': round(coco_ha, 1),
        'nombre_arbres_estime': nb_arbres,
        'densite_arbres_ha': DENSITE_ARBRES_HA,
        'production_noix_an': production_noix,
        'valeur_estimee_eur_an': round(valeur_eur, 0),
    }

    print(f"\n  --- ESTIMATIONS COCOTIERS ---")
    print(f"  Surface:    {coco_ha:.0f} ha")
    print(f"  Arbres:     ~{nb_arbres:,}")
    print(f"  Production: ~{production_noix:,} noix/an")
    print(f"  Valeur:     ~{valeur_eur:,.0f} EUR/an")

    return stats, estimations


# ============================================================================
# SORTIES
# ============================================================================

def create_map(composite, classified, roi):
    """Genere la carte Folium interactive."""
    print("Generation de la carte interactive...")

    center = [(GUADELOUPE_BBOX[1] + GUADELOUPE_BBOX[3]) / 2,
              (GUADELOUPE_BBOX[0] + GUADELOUPE_BBOX[2]) / 2]
    m = folium.Map(location=center, zoom_start=10)

    # Couche satellite RGB
    rgb_tiles = composite.select(['B4', 'B3', 'B2']).getMapId({
        'min': 0, 'max': 0.3, 'gamma': 1.4
    })
    folium.TileLayer(
        tiles=rgb_tiles['tile_fetcher'].url_format,
        attr='Sentinel-2 RGB',
        name='Satellite RGB',
        overlay=True
    ).add_to(m)

    # Couche classification
    palette = ['#000000',  # 0 = non classe
               '#2196F3',  # 1 = eau
               '#795548',  # 2 = sol nu
               '#9E9E9E',  # 3 = urbain
               '#FFC107',  # 4 = cultures
               '#4CAF50',  # 5 = cocotiers
               '#1B5E20',  # 6 = foret
               '#8BC34A']  # 7 = autre veg

    classif_tiles = classified.getMapId({
        'min': 0, 'max': 7, 'palette': palette
    })
    folium.TileLayer(
        tiles=classif_tiles['tile_fetcher'].url_format,
        attr='Classification',
        name='Classification',
        overlay=True
    ).add_to(m)

    # Couche cocotiers seuls (highlight)
    cocotiers_only = classified.eq(5).selfMask()
    coco_tiles = cocotiers_only.getMapId({
        'min': 0, 'max': 1, 'palette': ['#00000000', '#FF5722']
    })
    folium.TileLayer(
        tiles=coco_tiles['tile_fetcher'].url_format,
        attr='Cocotiers',
        name='COCOTIERS (zones detectees)',
        overlay=True,
        show=True
    ).add_to(m)

    # Legende HTML
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                background:white;padding:12px;border-radius:8px;
                box-shadow:0 2px 6px rgba(0,0,0,0.3);font-size:13px;">
        <b>Classification</b><br>
        <span style="color:#2196F3">&#9632;</span> Eau<br>
        <span style="color:#795548">&#9632;</span> Sol nu<br>
        <span style="color:#9E9E9E">&#9632;</span> Urbain<br>
        <span style="color:#FFC107">&#9632;</span> Cultures<br>
        <span style="color:#4CAF50">&#9632;</span> <b>Cocotiers</b><br>
        <span style="color:#1B5E20">&#9632;</span> Foret dense<br>
        <span style="color:#8BC34A">&#9632;</span> Autre vegetation
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl().add_to(m)

    output_file = OUTPUT_DIR / 'carte_classification.html'
    m.save(str(output_file))
    print(f"  Carte: {output_file}")
    return output_file


def save_results(stats, estimations):
    """Sauvegarde les resultats."""
    print("Sauvegarde des resultats...")

    # JSON
    results = {
        'date_analyse': datetime.now().isoformat(),
        'region': 'Guadeloupe',
        'source': 'Sentinel-2 SR Harmonized 2024',
        'resolution': '10m',
        'classification': stats,
        'estimations_cocotiers': estimations,
    }
    stats_file = OUTPUT_DIR / 'statistiques.json'
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"  Stats: {stats_file}")

    # Rapport texte
    rapport = []
    rapport.append("=" * 60)
    rapport.append("RAPPORT - DETECTION ZONES DE COCOTIERS")
    rapport.append("Guadeloupe - Sentinel-2 - 2024")
    rapport.append(f"Date: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    rapport.append("=" * 60)
    rapport.append("")
    rapport.append("CLASSIFICATION DU TERRITOIRE")
    rapport.append("-" * 40)
    for class_name, data in stats.items():
        rapport.append(f"  {class_name:30s} {data['hectares']:>8.0f} ha  ({data['km2']:.1f} km2)")
    rapport.append("")
    rapport.append("ESTIMATIONS COCOTIERS")
    rapport.append("-" * 40)
    rapport.append(f"  Surface detectee:      {estimations['surface_cocotiers_ha']:.0f} ha")
    rapport.append(f"  Densite estimee:       {estimations['densite_arbres_ha']} arbres/ha")
    rapport.append(f"  Nombre d'arbres:       ~{estimations['nombre_arbres_estime']:,}")
    rapport.append(f"  Production estimee:    ~{estimations['production_noix_an']:,} noix/an")
    rapport.append(f"  Valeur estimee:        ~{estimations['valeur_estimee_eur_an']:,.0f} EUR/an")
    rapport.append("")
    rapport.append("METHODOLOGIE")
    rapport.append("-" * 40)
    rapport.append("  Classification spectrale par regles sur indices NDVI, EVI, NDWI")
    rapport.append("  Filtrage morphologique (mode focal 30m)")
    rapport.append("  Source: Copernicus Sentinel-2 SR Harmonized")
    rapport.append("  Resolution: 10 metres")
    rapport.append("")
    rapport.append("LIMITES")
    rapport.append("-" * 40)
    rapport.append("  - Resolution 10m : detection de zones, pas d'arbres individuels")
    rapport.append("  - Confusion possible entre cocotiers et autres palmiers")
    rapport.append("  - Les estimations de densite sont des moyennes")
    rapport.append("  - Validation terrain recommandee")
    rapport.append("")

    rapport_file = OUTPUT_DIR / 'rapport.txt'
    with open(rapport_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(rapport))
    print(f"  Rapport: {rapport_file}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("DETECTION ZONES DE COCOTIERS - GUADELOUPE")
    print("=" * 60)
    print()

    OUTPUT_DIR.mkdir(exist_ok=True)

    # 1. Init
    initialize_ee()

    # 2. ROI
    roi = ee.Geometry.Rectangle(GUADELOUPE_BBOX)

    # 3. Composite
    composite = get_composite(roi)

    # 4. Indices spectraux
    indices = compute_indices(composite)

    # 5. Classification
    classified = classify(indices)

    # 6. Statistiques
    stats, estimations = compute_statistics(classified, roi)

    # 7. Carte
    create_map(composite, classified, roi)

    # 8. Sauvegarde
    save_results(stats, estimations)

    print()
    print("=" * 60)
    print("ANALYSE TERMINEE")
    print("=" * 60)
    print()
    print("Fichiers generes:")
    print("  - carte_classification.html  (carte interactive)")
    print("  - statistiques.json          (donnees structurees)")
    print("  - rapport.txt                (resume lisible)")
    print()


if __name__ == "__main__":
    main()
