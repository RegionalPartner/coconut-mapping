#!/usr/bin/env python3
"""
Enrichit le top 100 parcelles avec les donnees proprietaires DGFIP (personnes morales).
Croisement spatial RPG <-> Cadastre <-> DGFIP PM.
"""
import pandas as pd
import geopandas as gpd
import json
import gzip
import io
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / 'output_imagery'
RPG_DIR = (BASE_DIR / 'RPG_3-0__GPKG_RGAF09UTM20_R01_2024-01-01' / 'RPG'
           / '1_DONNEES_LIVRAISON_2024'
           / 'RPG_3-0__GPKG_RGAF09UTM20_R01_2024-01-01')

def main():
    # 1. Charger DGFIP personnes morales
    print("Chargement DGFIP personnes morales...")
    pm = pd.read_csv(OUTPUT_DIR / 'dgfip_personnes_morales_971.csv', sep=';', encoding='latin1', dtype=str)
    # Renommer les colonnes par index pour eviter les problemes d'encodage
    # Index: 0=Dept, 2=Code Commune, 4=Prefixe, 5=Section, 6=N plan,
    #        19=SIREN, 20=Groupe personne, 22=Forme juridique abregee, 23=Denomination
    cols = pm.columns.tolist()
    col_map = {
        cols[2]: 'code_commune',
        cols[4]: 'prefixe_raw',
        cols[5]: 'section_raw',
        cols[6]: 'nplan_raw',
        cols[19]: 'siren',
        cols[20]: 'groupe_personne',
        cols[22]: 'forme_juridique',
        cols[23]: 'denomination',
    }
    pm = pm.rename(columns=col_map)
    print(f"  {len(pm)} lignes")

    # Construire l'ID cadastral: 971 + code_commune(3) + prefixe(3) + section + n_plan(4)
    pm['code_com'] = pm['code_commune'].str.strip().str.zfill(3)
    pm['prefixe'] = pm['prefixe_raw'].fillna('000').str.strip().str.zfill(3)
    pm['section_col'] = pm['section_raw'].str.strip()
    pm['nplan'] = pm['nplan_raw'].str.strip().str.zfill(4)
    pm['id_cadastre'] = '971' + pm['code_com'] + pm['prefixe'] + pm['section_col'] + pm['nplan']

    # Deduplication : garder le premier proprietaire par parcelle cadastrale
    pm_unique = pm.drop_duplicates(subset='id_cadastre', keep='first')
    print(f"  {len(pm_unique)} parcelles uniques")

    # Index rapide
    pm_dict = {}
    for _, row in pm_unique.iterrows():
        pm_dict[row['id_cadastre']] = {
            'proprietaire': str(row.get('denomination', '')).strip(),
            'siren': str(row.get('siren', '')).strip(),
            'type_proprietaire': str(row.get('groupe_personne', '')).strip(),
            'forme_juridique': str(row.get('forme_juridique', '')).strip(),
        }

    # 2. Charger parcelles_analysis.json
    print("\nChargement parcelles_analysis.json...")
    with open(OUTPUT_DIR / 'parcelles_analysis.json', 'r', encoding='utf-8') as f:
        analysis = json.load(f)

    top = analysis['top_parcelles']
    top_ids = [p['id_parcel'] for p in top]
    print(f"  Top parcelles: {len(top)}")

    # 3. Charger RPG GPKG pour le top 100
    print("\nChargement RPG GPKG...")
    rpg = gpd.read_file(RPG_DIR / 'RPG_Parcelles.gpkg')
    rpg.columns = [c.lower() for c in rpg.columns]
    rpg = rpg.to_crs('EPSG:4326')

    rpg_top = rpg[rpg['id_parcel'].isin(top_ids)].copy()
    print(f"  {len(rpg_top)} parcelles RPG")

    # 4. Identifier les communes
    communes_geojson = gpd.read_file(BASE_DIR / 'communes_guadeloupe.geojson').to_crs('EPSG:4326')
    centroids_gdf = gpd.GeoDataFrame(
        rpg_top[['id_parcel']],
        geometry=rpg_top.geometry.centroid,
        crs='EPSG:4326'
    )
    joined = gpd.sjoin(centroids_gdf, communes_geojson[['code', 'geometry']], how='left', predicate='within')
    joined = joined[~joined.index.duplicated(keep='first')]
    rpg_top['code_insee'] = joined['code'].values

    codes_insee = rpg_top['code_insee'].dropna().unique()
    print(f"  Communes: {sorted(codes_insee)}")

    # 5. Telecharger cadastre par commune et croiser
    print("\nCroisement spatial RPG <-> Cadastre <-> DGFIP PM...")
    results = {}

    for code_insee in sorted(codes_insee):
        url = f'https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{code_insee}/geojson/parcelles'
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'CoconutMapping/1.0',
                'Accept-Encoding': 'gzip',
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                # Decompress gzip if needed
                if raw[:2] == b'\x1f\x8b':
                    raw = gzip.decompress(raw)
                cad_data = json.loads(raw)
            cad = gpd.GeoDataFrame.from_features(cad_data['features'], crs='EPSG:4326')
            print(f"  {code_insee}: {len(cad)} parcelles cadastrales")
        except Exception as e:
            print(f"  {code_insee}: ERREUR {e}")
            continue

        # Parcelles RPG dans cette commune
        rpg_commune = rpg_top[rpg_top['code_insee'] == code_insee]

        for idx, rpg_row in rpg_commune.iterrows():
            rpg_geom = rpg_row.geometry
            pid = rpg_row['id_parcel']

            # Trouver les parcelles cadastrales qui intersectent
            cad_intersects = cad[cad.geometry.intersects(rpg_geom)]
            if len(cad_intersects) == 0:
                continue

            # Prendre celle avec la plus grande intersection
            best_id = None
            best_area = 0
            for _, cad_row in cad_intersects.iterrows():
                try:
                    inter = rpg_geom.intersection(cad_row.geometry)
                    area = inter.area
                    if area > best_area:
                        best_area = area
                        best_id = cad_row.get('id', '')
                except Exception:
                    continue

            if best_id and best_id in pm_dict:
                info = pm_dict[best_id]
                denom = info['proprietaire']
                results[pid] = {
                    'proprietaire': denom if denom and denom != 'nan' else None,
                    'siren': info['siren'] if info['siren'] != 'nan' else None,
                    'type_proprietaire': info['type_proprietaire'] if info['type_proprietaire'] != 'nan' else None,
                    'forme_juridique': info['forme_juridique'] if info['forme_juridique'] != 'nan' else None,
                    'id_cadastre': best_id,
                }
            elif best_id:
                results[pid] = {
                    'proprietaire': None,
                    'siren': None,
                    'type_proprietaire': 'Personne physique (presume)',
                    'forme_juridique': None,
                    'id_cadastre': best_id,
                }

    # 6. Resultats
    print(f"\nResultats: {len(results)}/{len(top)} parcelles croisees")
    nb_pm = sum(1 for v in results.values() if v.get('proprietaire'))
    nb_pp = sum(1 for v in results.values() if not v.get('proprietaire'))
    print(f"  Personnes morales identifiees: {nb_pm}")
    print(f"  Personnes physiques (presume): {nb_pp}")

    print("\nPersonnes morales trouvees:")
    for pid, info in sorted(results.items()):
        if info.get('proprietaire'):
            print(f"  {pid}: {info['proprietaire']} ({info['forme_juridique']}) SIREN:{info['siren']}")

    # 7. Enrichir le JSON
    print("\nEnrichissement du JSON...")
    for p in analysis['top_parcelles']:
        pid = p['id_parcel']
        if pid in results:
            p['proprietaire'] = results[pid].get('proprietaire')
            p['siren'] = results[pid].get('siren')
            p['type_proprietaire'] = results[pid].get('type_proprietaire')
            p['forme_juridique'] = results[pid].get('forme_juridique')
            p['id_cadastre'] = results[pid].get('id_cadastre')
        else:
            p['proprietaire'] = None
            p['siren'] = None
            p['type_proprietaire'] = 'Non identifie'
            p['forme_juridique'] = None
            p['id_cadastre'] = None

    with open(OUTPUT_DIR / 'parcelles_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)

    print("JSON mis a jour!")

    # Stats finales
    total_pm = sum(1 for p in analysis['top_parcelles'] if p.get('proprietaire'))
    total_pp = sum(1 for p in analysis['top_parcelles'] if p.get('type_proprietaire') == 'Personne physique (presume)')
    total_ni = sum(1 for p in analysis['top_parcelles'] if p.get('type_proprietaire') == 'Non identifie')
    print(f"\n=== BILAN ===")
    print(f"  Personnes morales: {total_pm}")
    print(f"  Personnes physiques: {total_pp}")
    print(f"  Non identifie: {total_ni}")


if __name__ == '__main__':
    main()
