#!/usr/bin/env python3
"""
Genere un fichier HTML standalone multi-onglets du dashboard Coconut Mapping.
Inclut les 5 onglets : Vue Direction, Vue Technique, Donnees, Parcelles, Consolidation.
Les cartes Folium interactives sont embarquees en base64.
"""

import re
import os
import sys
import json
import base64
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / 'output_imagery'

# Import Flask app pour le rendu des templates
sys.path.insert(0, str(BASE_DIR))
from app import app as flask_app


def load_map_b64(filename):
    """Charge une carte HTML Folium et retourne en base64."""
    path = OUTPUT_DIR / filename
    if not path.exists():
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return base64.b64encode(f.read().encode('utf-8')).decode('ascii')


def extract_main(html):
    """Extrait le contenu entre <main> et </main>."""
    m = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL)
    return m.group(1).strip() if m else ''


def extract_user_scripts(html):
    """Extrait les <script> utilisateur (apres charts.js)."""
    m = re.search(r'charts\.js.*?</script>\s*(.*?)</body>', html, re.DOTALL)
    return m.group(1).strip() if m else ''


TABS = [
    ('client', '/client', 'Vue Direction'),
    ('technique', '/technique', 'Vue Technique'),
    ('donnees', '/donnees', 'Donnees'),
    ('parcelles', '/parcelles', 'Parcelles'),
    ('consolidation', '/consolidation', 'Consolidation'),
]


def main():
    print("=== Export Dashboard Standalone (5 onglets) ===\n")

    # Charger donnees JSON
    print("Chargement des donnees...")
    with open(OUTPUT_DIR / 'statistiques.json', 'r', encoding='utf-8') as f:
        stats = json.load(f)
    with open(OUTPUT_DIR / 'metadata.json', 'r', encoding='utf-8') as f:
        metadata = json.load(f)

    # Charger et encoder les cartes Folium en base64
    print("Encodage des cartes Folium...")
    map_b64 = {}
    for name in ['carte_parcelles.html', 'carte_classification.html', 'carte_guadeloupe.html', 'carte_top100.html']:
        b = load_map_b64(name)
        if b:
            map_b64[name] = b
            print(f"  {name}: {len(b) // 1024:,} Ko")
        else:
            print(f"  {name}: non trouve (carte ignoree)")

    # Charger assets statiques (CSS + JS)
    with open(BASE_DIR / 'static' / 'js' / 'charts.js', 'r', encoding='utf-8') as f:
        charts_js = f.read()
    with open(BASE_DIR / 'static' / 'css' / 'custom.css', 'r', encoding='utf-8') as f:
        custom_css = f.read()

    # Rendre chaque page via le test client Flask
    print("Rendu des templates Flask...")
    contents = {}
    user_scripts = {}

    with flask_app.test_client() as client:
        for tab_id, route, label in TABS:
            resp = client.get(route)
            if resp.status_code != 200:
                print(f"  ERREUR {route}: HTTP {resp.status_code}")
                contents[tab_id] = (
                    '<div class="bg-red-50 border-l-4 border-red-400 rounded-r-xl p-6">'
                    f'<p class="text-red-800">Erreur de rendu pour {label}</p></div>'
                )
                user_scripts[tab_id] = ''
                continue
            html = resp.data.decode('utf-8')
            contents[tab_id] = extract_main(html)
            user_scripts[tab_id] = extract_user_scripts(html)
            print(f"  {label}: OK ({len(contents[tab_id]):,} chars)")

    # Remplacer les iframe src="/maps/..." par data-map pour embedding local
    map_keys = {
        '/maps/carte_classification.html': 'carte_classification',
        '/maps/carte_parcelles.html': 'carte_parcelles',
        '/maps/carte_guadeloupe.html': 'carte_guadeloupe',
        '/maps/carte_top100.html': 'carte_top100',
    }
    for tab_id in contents:
        for src_url, key in map_keys.items():
            contents[tab_id] = contents[tab_id].replace(
                f'src="{src_url}"', f'data-map="{key}" src="about:blank"'
            )

    # --- Construction du HTML standalone ---
    print("Construction du HTML...")
    parts = []

    # <head>
    parts.append(f'''<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Coconut Mapping - Dashboard Complet</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
    <script>
      tailwind.config = {{
        theme: {{ extend: {{ colors: {{
          'coco': {{ 50: '#F0FDF4', 100: '#DCFCE7', 500: '#4CAF50', 700: '#1B5E20' }}
        }}}}}}
      }}
    </script>
    <style>
{custom_css}
.tab-btn {{
    padding: 10px 20px;
    border-radius: 8px 8px 0 0;
    font-size: 14px;
    font-weight: 500;
    color: #bbf7d0;
    background: transparent;
    border: none;
    cursor: pointer;
    transition: all 0.15s ease;
}}
.tab-btn:hover {{ background: rgba(255,255,255,0.1); }}
.tab-btn.active-tab {{ background: #f9fafb; color: #166534; }}
iframe {{ border: none; display: block; }}
    </style>
</head>
<body class="bg-gray-50 min-h-screen flex flex-col">''')

    # Header + navigation onglets
    tab_btns = []
    for i, (tab_id, _, label) in enumerate(TABS):
        cls = ' active-tab' if i == 0 else ''
        tab_btns.append(
            f'            <button class="tab-btn{cls}" data-tab="{tab_id}" '
            f"onclick=\"switchTab('{tab_id}')\">{label}</button>"
        )

    parts.append(f'''
    <header class="bg-gradient-to-r from-green-900 to-green-700 text-white shadow-lg">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 py-5 flex items-center justify-between">
            <div>
                <h1 class="text-2xl font-bold tracking-tight">Coconut Mapping</h1>
                <p class="text-green-300 text-sm mt-0.5">Guadeloupe &mdash; Dashboard complet (standalone)</p>
            </div>
            <div class="text-right text-sm text-green-300 hidden sm:block">
                <div>{metadata.get('date', '')[:10]}</div>
                <div>{metadata.get('images_used', '')} images Sentinel-2</div>
            </div>
        </div>
        <nav class="max-w-7xl mx-auto px-4 sm:px-6 flex gap-1 flex-wrap">
{chr(10).join(tab_btns)}
        </nav>
    </header>''')

    # Contenu principal : panels par onglet (tous visibles au chargement pour le rendu Chart.js)
    parts.append('\n    <main class="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 py-8">')
    for i, (tab_id, _, _) in enumerate(TABS):
        active = ' active' if i == 0 else ''
        parts.append(f'    <div id="tab-{tab_id}" class="tab-panel{active}">')
        parts.append(contents[tab_id])
        parts.append('    </div>')
    parts.append('    </main>')

    # Footer
    parts.append('''
    <footer class="bg-gray-100 border-t border-gray-200 mt-auto">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 py-4 text-xs text-gray-500 flex flex-wrap gap-x-6 gap-y-1">
            <span>Source : Copernicus Sentinel-2 SR Harmonized + RPG 2024</span>
            <span>Traitement : Google Earth Engine + Python</span>
            <span>Coconut Mapping &mdash; Guadeloupe 2024</span>
        </div>
    </footer>''')

    # Script : injection des donnees STATS / CLASSIFICATION
    parts.append('\n    <script>')
    parts.append(f'const STATS = {json.dumps(stats)};')
    parts.append('const CLASSIFICATION = STATS.classification || {};')
    parts.append('const ESTIMATIONS = STATS.estimations_cocotiers || {};')
    parts.append('    </script>')

    # Script : charts.js inline
    parts.append('\n    <script>')
    parts.append(charts_js)
    parts.append('    </script>')

    # Script : donnees des cartes Folium en base64
    parts.append('\n    <script>')
    parts.append('const MAP_DATA = {')
    for filename, b64 in map_b64.items():
        key = filename.replace('.html', '')
        parts.append(f'  "{key}": "{b64}",')
    parts.append('};')
    # Fonction d'embedding des cartes dans les iframes
    parts.append('function embedMaps() {')
    parts.append('    document.querySelectorAll("iframe[data-map]").forEach(function(iframe) {')
    parts.append('        var key = iframe.getAttribute("data-map");')
    parts.append('        if (MAP_DATA[key]) {')
    parts.append('            var h = atob(MAP_DATA[key]);')
    parts.append('            var blob = new Blob([h], {type: "text/html"});')
    parts.append('            iframe.src = URL.createObjectURL(blob);')
    parts.append('        }')
    parts.append('    });')
    parts.append('}')
    parts.append('    </script>')

    # Scripts utilisateur de chaque onglet (deja dans des balises <script>)
    for tab_id, _, label in TABS:
        if user_scripts[tab_id]:
            parts.append(f'\n    <!-- Scripts: {label} -->')
            parts.append(user_scripts[tab_id])

    # Script : navigation par onglets + initialisation
    parts.append('''
    <script>
function switchTab(tabId) {
    document.querySelectorAll('.tab-panel').forEach(function(p) { p.style.display = 'none'; });
    document.querySelectorAll('.tab-btn').forEach(function(b) { b.classList.remove('active-tab'); });
    document.getElementById('tab-' + tabId).style.display = 'block';
    document.querySelector('[data-tab="' + tabId + '"]').classList.add('active-tab');
}
document.addEventListener('DOMContentLoaded', function() {
    // Charger les cartes Folium dans les iframes
    embedMaps();
    // Masquer les onglets non-actifs apres le rendu des graphiques Chart.js
    setTimeout(function() {
        document.querySelectorAll('.tab-panel:not(.active)').forEach(function(p) {
            p.style.display = 'none';
        });
    }, 300);
});
    </script>''')

    parts.append('\n</body>\n</html>')

    # Assembler et ecrire le fichier
    html = '\n'.join(parts)
    output_path = OUTPUT_DIR / 'dashboard_standalone.html'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\nFichier genere : {output_path}")
    print(f"Taille : {size_mb:.1f} Mo")
    print(f"Onglets : {', '.join(l for _, _, l in TABS)}")
    print(f"Cartes embarquees : {', '.join(map_b64.keys()) if map_b64 else 'aucune'}")
    print("\nOuvrir dans un navigateur pour verifier.")


if __name__ == '__main__':
    main()
