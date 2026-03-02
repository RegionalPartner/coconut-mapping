#!/usr/bin/env python3
"""Verifie les transactions DVF pour les 100 meilleures parcelles."""
import json, urllib.request, time
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / 'output_imagery'

with open(OUTPUT_DIR / 'parcelles_analysis.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

top = data['top_parcelles'][:100]

# Collecter les refs cadastrales
parcels = []
for p in top:
    idc = p.get('id_cadastre')
    if idc and len(idc) >= 14:
        parcels.append({
            'id_parcel': p['id_parcel'],
            'id_cadastre': idc,
            'code_commune': idc[:5],
            'section_prefixe': idc[5:10],
            'num_parcelle': idc[10:],
            'score': p.get('score_potentiel', ''),
            'commune': p.get('commune', ''),
        })

print(f"Parcelles avec ref cadastrale: {len(parcels)}/100")

# Grouper par commune+section
sections = {}
for p in parcels:
    key = (p['code_commune'], p['section_prefixe'])
    if key not in sections:
        sections[key] = []
    sections[key].append(p)

print(f"Sections cadastrales a interroger: {len(sections)}")
print()

# Interroger l'API DVF Etalab
found = []
no_match = 0
errors = 0

for i, ((code_commune, section_pref), parcel_list) in enumerate(sorted(sections.items())):
    url = f"https://app.dvf.etalab.gouv.fr/api/mutations3/{code_commune}/{section_pref}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'CoconutMapping/1.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = json.loads(resp.read())
            mutations = raw.get('mutations', raw) if isinstance(raw, dict) else raw
    except Exception as e:
        print(f"  ERREUR {code_commune}/{section_pref}: {e}")
        errors += 1
        continue

    for p in parcel_list:
        target_id = p['id_cadastre']
        matches = [m for m in mutations if m.get('id_parcelle') == target_id]

        if matches:
            matches.sort(key=lambda x: x.get('date_mutation', ''), reverse=True)
            latest = matches[0]
            found.append({
                'id_parcel': p['id_parcel'],
                'id_cadastre': target_id,
                'commune': p['commune'],
                'score': p['score'],
                'date': latest.get('date_mutation', '?'),
                'prix': latest.get('valeur_fonciere', '?'),
                'nature': latest.get('nature_mutation', '?'),
                'type_terrain': latest.get('nature_culture', '?'),
                'surface': latest.get('surface_terrain', '?'),
                'nb_mutations': len(matches),
            })
        else:
            no_match += 1

    if (i + 1) % 10 == 0:
        print(f"  {i+1}/{len(sections)} sections traitees...")
    time.sleep(0.3)

print()
print("=" * 120)
print(f"RESULTATS DVF — Top 100 parcelles")
print("=" * 120)
print(f"Parcelles avec transaction: {len(found)}")
print(f"Parcelles sans transaction: {no_match}")
print(f"Erreurs API: {errors}")
print()

if found:
    print("TRANSACTIONS TROUVEES:")
    print("-" * 120)
    for f in sorted(found, key=lambda x: x['date'], reverse=True):
        prix = f['prix']
        if prix and prix != '?' and str(prix) != 'nan':
            try:
                prix_str = f"{float(prix):,.0f} EUR"
            except ValueError:
                prix_str = str(prix)
        else:
            prix_str = "?"
        surf = f['surface']
        if surf and str(surf) != 'nan':
            try:
                surf_str = f"{float(surf):,.0f} m2"
            except ValueError:
                surf_str = str(surf)
        else:
            surf_str = "?"
        print(f"  {f['id_parcel']} | Cad: {f['id_cadastre']} | {f['commune']} | "
              f"Score {f['score']} | {f['date']} | {prix_str} | {f['nature']} | "
              f"{f['type_terrain']} | {surf_str} | ({f['nb_mutations']} mutation(s))")
else:
    print("Aucune transaction trouvee pour les 100 parcelles.")
