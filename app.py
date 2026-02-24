"""Dashboard web - Coconut Mapping Guadeloupe."""
from flask import Flask, render_template, send_from_directory, jsonify, redirect, url_for
import json
from pathlib import Path

app = Flask(__name__)
DATA_DIR = Path('output_imagery')

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


def load_data():
    with open(DATA_DIR / 'statistiques.json', 'r', encoding='utf-8') as f:
        stats = json.load(f)
    with open(DATA_DIR / 'metadata.json', 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    return stats, metadata


def load_consolidation():
    """Charge les donnees de consolidation si disponibles."""
    conso_file = DATA_DIR / 'consolidation.json'
    if conso_file.exists():
        with open(conso_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


def load_parcelles():
    """Charge les donnees d'analyse parcellaire si disponibles."""
    parc_file = DATA_DIR / 'parcelles_analysis.json'
    if parc_file.exists():
        with open(parc_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


@app.route('/')
def index():
    return redirect(url_for('client'))


@app.route('/client')
def client():
    stats, metadata = load_data()
    conso = load_consolidation()
    return render_template('client.html', stats=stats, metadata=metadata,
                           conso=conso, active='client')


@app.route('/technique')
def technique():
    stats, metadata = load_data()
    conso = load_consolidation()
    return render_template('technique.html', stats=stats, metadata=metadata,
                           thresholds=THRESHOLDS, conso=conso, active='technique')


@app.route('/donnees')
def donnees():
    stats, metadata = load_data()
    conso = load_consolidation()
    return render_template('donnees.html', stats=stats, metadata=metadata,
                           thresholds=THRESHOLDS, conso=conso, active='donnees')


@app.route('/parcelles')
def parcelles():
    stats, metadata = load_data()
    conso = load_consolidation()
    parc = load_parcelles()
    return render_template('parcelles.html', stats=stats, metadata=metadata,
                           conso=conso, parc=parc, active='parcelles')


@app.route('/consolidation')
def consolidation():
    stats, metadata = load_data()
    conso = load_consolidation()
    return render_template('consolidation.html', stats=stats, metadata=metadata,
                           conso=conso, active='consolidation')


@app.route('/api/parcelles')
def api_parcelles():
    parc = load_parcelles()
    if parc:
        return jsonify(parc)
    return jsonify({'error': 'Analyse parcellaire non disponible'}), 404


@app.route('/api/consolidation')
def api_consolidation():
    conso = load_consolidation()
    if conso:
        return jsonify(conso)
    return jsonify({'error': 'Consolidation non disponible'}), 404


@app.route('/maps/<path:filename>')
def serve_map(filename):
    return send_from_directory(str(DATA_DIR), filename)


@app.route('/api/data')
def api_data():
    stats, metadata = load_data()
    return jsonify({**stats, 'metadata': metadata})


if __name__ == '__main__':
    print()
    print("  Coconut Mapping Dashboard")
    print("  http://localhost:5000")
    print()
    app.run(debug=True, port=5000)
