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
    # Get the JSON data from the request
    data = request.json
    
    if 'file' not in data or 'well_column' not in data:
        return jsonify({
            "status": "error",
            "message": "Missing 'file' or 'well_column' in request."
        })

    # Extract file and well_column from the JSON
    base64_file = data['file']
    well_column = data['well_column']

    try:
        # Decode the base64 file content back to binary
        file_data = base64.b64decode(base64_file)
        user_file = io.BytesIO(file_data)
        
        # Read the Excel file into a pandas DataFrame
        user_df = pd.read_excel(user_file)
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to read uploaded Excel file: {str(e)}"
        })

    # Check if the well_column exists in the user's file
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

    # Convert results to DataFrame
    result_df = pd.DataFrame(results)

    # Save result as an Excel file to memory
    output = io.BytesIO()
    result_df.to_excel(output, index=False)
    output.seek(0)

    # Encode the result Excel file in Base64 format
    encoded_file = base64.b64encode(output.read()).decode('utf-8')

    # Return response with the encoded file content
    return jsonify({
        "status": "success",
        "fileName": "matched_wells.xlsx",
        "fileContent": encoded_file
    })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
