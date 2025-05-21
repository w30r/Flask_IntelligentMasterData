from flask import Flask, request, jsonify
import pandas as pd
from rapidfuzz import fuzz, process
import io
import base64
import os

app = Flask(__name__)

# Load MasterWells.xlsx
MASTER_WELLS_FILE = "MasterWells.xlsx"
MASTER_DF = pd.read_excel(MASTER_WELLS_FILE)
MASTER_WELL_NAMES = MASTER_DF["Well Name"].dropna().astype(str).tolist()

@app.route('/match-wells', methods=['POST'])
def match_wells():
    if 'file' not in request.files or 'well_column' not in request.form:
        return jsonify({
            "status": "error",
            "message": "Missing 'file' or 'well_column' in request."
        })

    user_file = request.files['file']
    well_column = request.form['well_column']

    try:
        user_df = pd.read_excel(user_file)
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to read uploaded Excel file: {str(e)}"
        })

    if well_column not in user_df.columns:
        return jsonify({
            "status": "error",
            "message": f"Column '{well_column}' not found in uploaded file."
        })

    # Fuzzy matching
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

    # Save to memory and encode
    output = io.BytesIO()
    result_df.to_excel(output, index=False)
    output.seek(0)
    encoded_file = base64.b64encode(output.read()).decode('utf-8')

    return jsonify({
        "status": "success",
        "fileName": "matched_wells.xlsx",
        "fileContent": encoded_file
    })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
