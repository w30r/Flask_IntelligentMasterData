from flask import Flask, request, send_file, jsonify
import pandas as pd
from rapidfuzz import fuzz, process
import tempfile
import os

app = Flask(__name__)

# Load MasterWells.xlsx once at startup
MASTER_WELLS_FILE = "MasterWells.xlsx"
MASTER_DF = pd.read_excel(MASTER_WELLS_FILE)
MASTER_WELL_NAMES = MASTER_DF["Well Name"].dropna().astype(str).tolist()

@app.route('/match-wells', methods=['POST'])
def match_wells():
    if 'file' not in request.files or 'well_column' not in request.form:
        return jsonify({'error': 'Missing file or well_column parameter'}), 400

    user_file = request.files['file']
    well_column = request.form['well_column']

    try:
        user_df = pd.read_excel(user_file)
    except Exception as e:
        return jsonify({'error': f'Failed to read uploaded Excel file: {e}'}), 400

    if well_column not in user_df.columns:
        return jsonify({'error': f"Column '{well_column}' not found in uploaded file"}), 400

    user_wells = user_df[well_column].dropna().astype(str).tolist()

    results = []
    for well in user_wells:
        match, score, _ = process.extractOne(well, MASTER_WELL_NAMES, scorer=fuzz.token_sort_ratio)
        results.append({
            'User Well Name': well,
            'Matched Master Well Name': match,
            'Similarity Score': score
        })

    result_df = pd.DataFrame(results)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        result_df.to_excel(tmp.name, index=False)
        tmp.seek(0)
        return send_file(tmp.name, as_attachment=True, download_name="matched_wells.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__ == '__main__':
    app.run(debug=True)
