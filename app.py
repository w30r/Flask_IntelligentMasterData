from flask import Flask, request, jsonify
import pandas as pd
from rapidfuzz import fuzz, process
import base64, io, uuid, threading, os
from datetime import datetime
# from openpyxl import load_workbook
# from openpyxl.utils import get_column_letter

app = Flask(__name__)

# Load master well list once
MASTER_DF = pd.read_excel("MasterWells.xlsx")
MASTER_WELL_NAMES = MASTER_DF["Well Name"].dropna().astype(str).tolist()

# In-memory job store
jobs = {}
# Global list to store matched results for OS
well_mapping_json_library = []

# Background processing function
def process_file_async(job_id, base64_file, well_column):
    try:
        file_data = base64.b64decode(base64_file)
        user_file = io.BytesIO(file_data)
        user_df = pd.read_excel(user_file)

        if well_column not in user_df.columns:
            jobs[job_id] = {"status": "error", "message": f"Column '{well_column}' not found"}
            print(f"[❌] Job {job_id} failed: column '{well_column}' not found")
            return

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
        output = io.BytesIO()
        result_df.to_excel(output, index=False)
        output.seek(0)

        encoded_result = base64.b64encode(output.read()).decode("utf-8")

        matches_over_90 = sum(1 for r in results if r["Similarity Score"] >= 90)
        matches_below_90 = len(results) - matches_over_90
        percent_high_quality = round(matches_over_90 / len(results) * 100, 2)

        jobs[job_id].update({
            "status": "done",
            "fileContent": encoded_result,
            "fileName": "matched_wells.xlsx",
            "total_wells": len(results),
            "matches_over_90": matches_over_90,
            "matches_below_90": matches_below_90,
            "percent_high_quality": percent_high_quality
        })

        # Extend global JSON list for matches >= 90
        file_label = jobs[job_id].get("submitted_file_name", f"Job_{job_id}").rsplit(".", 1)[0]

        for r in results:
            if r["Similarity Score"] >= 90:
                well_mapping_json_library.append({
                    "User Well Name": r["User Well Name"],
                    "Matched Master Well Name": r["Matched Master Well Name"],
                    "Similarity Score": r["Similarity Score"],
                    "FileName": file_label
                })

        # file_label = jobs[job_id].get("submitted_file_name", f"Job_{job_id}").rsplit(".", 1)[0]
        # update_well_mapping_library(file_label, results)


        print(f"[✅] Job {job_id} completed successfully.")
    except Exception as e:
        jobs[job_id] = {"status": "error", "message": str(e)}
        print(f"[❌] Job {job_id} failed with error: {e}")

# def update_well_mapping_library(file_label, results):
#     LIBRARY_PATH = "WellMappingLibrary.xlsx"

#     if not os.path.exists(LIBRARY_PATH):
#         base_df = pd.DataFrame(sorted(set(MASTER_WELL_NAMES)), columns=["Well Name"])
#         base_df.to_excel(LIBRARY_PATH, index=False)

#     # Load existing library
#     lib_df = pd.read_excel(LIBRARY_PATH)

#     # Map from well name → matched name
#     mapping_dict = {
#         r["Matched Master Well Name"]: r["User Well Name"]
#         for r in results if r["Similarity Score"] >= 90
#     }

#     # Add new column
#     new_column_name = file_label
#     if new_column_name in lib_df.columns:
#         count = 2
#         while f"{new_column_name}_v{count}" in lib_df.columns:
#             count += 1
#         new_column_name = f"{new_column_name}_v{count}"

#     lib_df[new_column_name] = lib_df["Well Name"].map(mapping_dict)

#     lib_df.to_excel(LIBRARY_PATH, index=False)

# Submit endpoint
@app.route('/submit-task', methods=['POST'])
def submit_task():
    data = request.json

    if not data or 'file' not in data or 'well_column' not in data:
        return jsonify({"status": "error", "message": "Missing 'file' or 'well_column'"}), 400

    file_name = data.get("file_name", "uploaded_file.xlsx")  # Default if not provided
    job_id = str(uuid.uuid4())

    jobs[job_id] = {
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
        "submitted_file_name": file_name  # 💾 store it
    }

    thread = threading.Thread(
        target=process_file_async,
        args=(job_id, data['file'], data['well_column'])
    )
    thread.start()

    return jsonify({"status": "submitted", "job_id": job_id}), 202


# Poll result endpoint
@app.route('/get-result/<job_id>', methods=['GET'])
def get_result(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "error", "message": "Job ID not found"}), 200

    return jsonify(job)

@app.route('/list-jobs', methods=['GET'])
def list_jobs():
    job_list = []

    for job_id, job in jobs.items():
        job_summary = {
            "job_id": job_id,
            "status": job["status"],
            "created_at": job.get("created_at"),
            "submitted_file_name": job.get("submitted_file_name"),
            "total_wells": job.get("total_wells"),
            "matches_over_90": job.get("matches_over_90"),
            "matches_below_90": job.get("matches_below_90"),
            "percent_high_quality": job.get("percent_high_quality")
        }
        job_list.append(job_summary)

    return jsonify(job_list)




@app.route('/extract-headers', methods=['POST'])
def extract_headers():
    data = request.json
    if not data or 'file' not in data:
        return jsonify({"status": "error", "message": "Missing 'file' in request"}), 400

    try:
        # Decode base64 and read Excel
        file_data = base64.b64decode(data['file'])
        file_stream = io.BytesIO(file_data)
        df = pd.read_excel(file_stream)

        # Get column headers
        headers = df.columns.tolist()

        return jsonify({"status": "success", "headers": headers})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/download-library', methods=['GET'])
def download_library():
    LIBRARY_PATH = "WellMappingLibrary.xlsx"
    
    if not os.path.exists(LIBRARY_PATH):
        return jsonify({"status": "error", "message": "Library file not found."}), 404

    try:
        with open(LIBRARY_PATH, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")

        return jsonify({
            "status": "success",
            "fileName": "WellMappingLibrary.xlsx",
            "fileContent": encoded
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get-library-os', methods=['GET'])
def get_library_os():
    return jsonify(well_mapping_json_library)


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
