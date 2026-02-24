"""Genere une carte Folium standalone de l'apercu Sentinel-2 Guadeloupe."""
import ee
import json
import folium

# Init Earth Engine
SERVICE_ACCOUNT_FILE = 'gen-lang-client-0363696684-80e30fa97142.json'
with open(SERVICE_ACCOUNT_FILE, 'r') as f:
    sa_info = json.load(f)
credentials = ee.ServiceAccountCredentials(
    email=sa_info['client_email'],
    key_file=SERVICE_ACCOUNT_FILE
)
ee.Initialize(credentials)

# Zone Guadeloupe
bbox = [-61.81, 15.83, -61.00, 16.52]
roi = ee.Geometry.Rectangle(bbox)

# Composite Sentinel-2
def mask_clouds(image):
    qa = image.select('QA60')
    mask = qa.bitwiseAnd(1 << 10).eq(0).And(qa.bitwiseAnd(1 << 11).eq(0))
    return image.updateMask(mask).divide(10000)

collection = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
    .filterBounds(roi)
    .filterDate('2024-01-01', '2024-12-31')
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
    .map(mask_clouds))

composite = collection.median()
ndvi = composite.normalizedDifference(['B8', 'B4']).rename('NDVI')

# Tuiles Earth Engine
rgb_tiles = composite.select(['B4', 'B3', 'B2']).getMapId({
    'min': 0, 'max': 0.3, 'gamma': 1.4
})
ndvi_tiles = ndvi.getMapId({
    'min': -0.2, 'max': 0.8,
    'palette': ['brown', 'yellow', 'green', 'darkgreen']
})

# Carte Folium
center = [(bbox[1] + bbox[3]) / 2, (bbox[0] + bbox[2]) / 2]
m = folium.Map(location=center, zoom_start=10)

folium.TileLayer(
    tiles=rgb_tiles['tile_fetcher'].url_format,
    attr='Google Earth Engine / Sentinel-2',
    name='Satellite RGB',
    overlay=True
).add_to(m)

folium.TileLayer(
    tiles=ndvi_tiles['tile_fetcher'].url_format,
    attr='NDVI',
    name='NDVI Vegetation',
    overlay=True,
    show=False
).add_to(m)

folium.LayerControl().add_to(m)

m.save('output_imagery/carte_guadeloupe.html')
print("Carte sauvegardee: output_imagery/carte_guadeloupe.html")
