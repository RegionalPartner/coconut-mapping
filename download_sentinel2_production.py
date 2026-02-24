#!/usr/bin/env python3
"""
SCRIPT PRODUCTION - Téléchargement Sentinel-2 Guadeloupe
À EXÉCUTER EN LOCAL (WSL2/Ubuntu)

Utilise le Service Account: 204372217819-compute@developer.gserviceaccount.com
Project: gen-lang-client-0363696684
"""

import ee
import json
import geemap
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# Chemin vers le service account (à adapter)
SERVICE_ACCOUNT_FILE = 'gen-lang-client-0363696684-80e30fa97142.json'

# Zone d'intérêt: Guadeloupe [ouest, sud, est, nord]
GUADELOUPE_BBOX = [-61.81, 15.83, -61.00, 16.52]

# Période d'analyse
START_DATE = '2024-01-01'
END_DATE = '2024-12-31'

# Couverture nuageuse maximale (%)
MAX_CLOUD_COVER = 20

# Résolution d'export (mètres)
SCALE = 10  # 10m pour Sentinel-2

# Dossier de sortie
OUTPUT_DIR = Path('output_imagery')

# ============================================================================
# FONCTIONS
# ============================================================================

def initialize_earth_engine():
    """Initialise Earth Engine avec le service account."""
    print("🔌 Initialisation Google Earth Engine...")
    
    try:
        with open(SERVICE_ACCOUNT_FILE, 'r') as f:
            service_account_info = json.load(f)
        
        credentials = ee.ServiceAccountCredentials(
            email=service_account_info['client_email'],
            key_file=SERVICE_ACCOUNT_FILE
        )
        
        ee.Initialize(credentials)
        print(f"   ✓ Connecté avec: {service_account_info['client_email']}")
        print(f"   ✓ Project: {service_account_info['project_id']}")
        return True
        
    except Exception as e:
        print(f"   ✗ Erreur d'initialisation: {e}")
        return False


def mask_clouds(image):
    """Masque les nuages et cirrus dans une image Sentinel-2."""
    qa = image.select('QA60')
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11
    
    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(
        qa.bitwiseAnd(cirrus_bit_mask).eq(0)
    )
    
    return image.updateMask(mask).divide(10000)


def add_indices(image):
    """Ajoute des indices de végétation utiles."""
    # NDVI (Normalized Difference Vegetation Index)
    ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
    
    # EVI (Enhanced Vegetation Index)
    evi = image.expression(
        '2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))', {
            'NIR': image.select('B8'),
            'RED': image.select('B4'),
            'BLUE': image.select('B2')
        }
    ).rename('EVI')
    
    return image.addBands([ndvi, evi])


def get_sentinel2_composite(roi, start_date, end_date, max_cloud):
    """Crée un composite Sentinel-2 sans nuages."""
    print("\n📡 Chargement de la collection Sentinel-2...")
    
    collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                 .filterBounds(roi)
                 .filterDate(start_date, end_date)
                 .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', max_cloud))
                 .map(mask_clouds)
                 .map(add_indices))
    
    count = collection.size().getInfo()
    print(f"   ✓ {count} images trouvées")
    
    if count == 0:
        print("   ⚠ Aucune image. Essai avec plus de nuages...")
        collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                     .filterBounds(roi)
                     .filterDate(start_date, end_date)
                     .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 50))
                     .map(mask_clouds)
                     .map(add_indices))
        count = collection.size().getInfo()
        print(f"   ✓ {count} images avec critères élargis")
    
    # Créer le composite médian
    composite = collection.median()
    
    return composite, count


def calculate_statistics(composite, roi):
    """Calcule des statistiques sur le composite."""
    print("\n📊 Calcul des statistiques...")
    
    try:
        stats = composite.select(['NDVI', 'EVI']).reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=roi,
            scale=100,
            maxPixels=1e9
        ).getInfo()
        
        print(f"   ✓ NDVI moyen: {stats.get('NDVI', 0):.3f}")
        print(f"   ✓ EVI moyen: {stats.get('EVI', 0):.3f}")
        
        return stats
        
    except Exception as e:
        print(f"   ⚠ Calcul échoué: {e}")
        return {}


def export_to_drive(composite, roi, description):
    """Exporte l'image vers Google Drive."""
    print(f"\n💾 Export vers Google Drive: {description}")
    
    # Sélectionner les bandes importantes
    bands = ['B2', 'B3', 'B4', 'B8', 'NDVI', 'EVI']
    
    task = ee.batch.Export.image.toDrive(
        image=composite.select(bands),
        description=description,
        folder='GEE_Guadeloupe',
        scale=SCALE,
        region=roi,
        maxPixels=1e13,
        fileFormat='GeoTIFF'
    )
    
    task.start()
    
    print(f"   ✓ Export démarré!")
    print(f"   → ID: {task.id}")
    print(f"   → Suivi: https://code.earthengine.google.com/tasks")
    print(f"\n   📁 Le fichier apparaîtra dans Google Drive/GEE_Guadeloupe/")
    
    return task


def download_thumbnail(composite, roi, output_path):
    """Télécharge un aperçu de l'image."""
    print(f"\n🖼️  Téléchargement de l'aperçu...")
    
    try:
        url = composite.select(['B4', 'B3', 'B2']).getThumbURL({
            'region': roi,
            'dimensions': 2048,
            'format': 'png',
            'min': 0,
            'max': 0.3
        })
        
        print(f"   ✓ Aperçu disponible:")
        print(f"   {url}")
        
        # Sauvegarder l'URL
        url_file = output_path / 'preview_url.txt'
        with open(url_file, 'w') as f:
            f.write(url)
        
        print(f"   ✓ URL sauvegardée: {url_file}")
        
        return url
        
    except Exception as e:
        print(f"   ⚠ Échec: {e}")
        return None


def create_visualization_map(composite, roi, output_path):
    """Crée une carte de visualisation interactive."""
    print(f"\n🗺️  Création de la carte interactive...")
    
    try:
        Map = geemap.Map()
        
        # Ajouter le composite RGB
        vis_rgb = {
            'bands': ['B4', 'B3', 'B2'],
            'min': 0,
            'max': 0.3,
            'gamma': 1.4
        }
        Map.addLayer(composite, vis_rgb, 'RGB')
        
        # Ajouter NDVI
        vis_ndvi = {
            'bands': ['NDVI'],
            'min': -0.2,
            'max': 0.8,
            'palette': ['brown', 'yellow', 'green', 'darkgreen']
        }
        Map.addLayer(composite, vis_ndvi, 'NDVI')
        
        # Centrer sur Guadeloupe
        Map.centerObject(roi, zoom=10)
        
        # Sauvegarder
        map_file = output_path / 'preview_map.html'
        Map.save(str(map_file))
        
        print(f"   ✓ Carte sauvegardée: {map_file}")
        print(f"   → Ouvre ce fichier dans un navigateur")
        
        return map_file
        
    except Exception as e:
        print(f"   ⚠ Échec: {e}")
        return None


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Fonction principale."""
    print("="*70)
    print("🥥 TÉLÉCHARGEMENT SENTINEL-2 - GUADELOUPE")
    print("="*70)
    print()
    print(f"📅 Période: {START_DATE} → {END_DATE}")
    print(f"☁️  Nuages max: {MAX_CLOUD_COVER}%")
    print(f"📏 Résolution: {SCALE}m")
    print()
    
    # Créer dossier de sortie
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Initialiser Earth Engine
    if not initialize_earth_engine():
        print("\n❌ Impossible de continuer sans connexion Earth Engine")
        return
    
    # Définir la ROI
    print("\n📍 Zone d'intérêt: Guadeloupe")
    roi = ee.Geometry.Rectangle(GUADELOUPE_BBOX)
    area_km2 = roi.area(1).divide(1e6).getInfo()
    print(f"   ✓ Surface: {area_km2:.0f} km²")
    
    # Obtenir le composite
    composite, image_count = get_sentinel2_composite(
        roi, START_DATE, END_DATE, MAX_CLOUD_COVER
    )
    
    # Calculer statistiques
    stats = calculate_statistics(composite, roi)
    
    # Télécharger aperçu
    preview_url = download_thumbnail(composite, roi, OUTPUT_DIR)
    
    # Créer carte de visualisation
    map_file = create_visualization_map(composite, roi, OUTPUT_DIR)
    
    # Exporter vers Google Drive
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    description = f"Guadeloupe_S2_{timestamp}"
    task = export_to_drive(composite, roi, description)
    
    # Sauvegarder les métadonnées
    metadata = {
        'date': datetime.now().isoformat(),
        'region': 'Guadeloupe',
        'bbox': GUADELOUPE_BBOX,
        'area_km2': area_km2,
        'start_date': START_DATE,
        'end_date': END_DATE,
        'max_cloud_cover': MAX_CLOUD_COVER,
        'images_used': image_count,
        'scale_meters': SCALE,
        'bands': ['B2', 'B3', 'B4', 'B8', 'NDVI', 'EVI'],
        'statistics': stats,
        'export_id': task.id if task else None,
        'preview_url': preview_url
    }
    
    metadata_file = OUTPUT_DIR / 'metadata.json'
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n   ✓ Métadonnées: {metadata_file}")
    
    # Résumé
    print("\n" + "="*70)
    print("✅ PROCESSUS TERMINÉ")
    print("="*70)
    print(f"\n📁 Dossier de sortie: {OUTPUT_DIR}")
    print(f"\nFichiers générés:")
    print(f"  • metadata.json - Métadonnées complètes")
    print(f"  • preview_url.txt - URL de l'aperçu")
    print(f"  • preview_map.html - Carte interactive")
    print(f"\n🔄 Export Google Drive:")
    print(f"  • Nom: {description}")
    print(f"  • Dossier: GEE_Guadeloupe/")
    print(f"  • Suivi: https://code.earthengine.google.com/tasks")
    print(f"\n⏳ L'export prendra 5-15 minutes selon la taille.")
    print(f"   Le fichier GeoTIFF sera dans ton Google Drive.")
    print()
    print("📖 Prochaines étapes:")
    print("  1. Vérifie l'avancement sur code.earthengine.google.com/tasks")
    print("  2. Télécharge le GeoTIFF depuis Google Drive")
    print("  3. Lance la détection de cocotiers sur l'image")
    print()


if __name__ == "__main__":
    main()
