from flask import Flask, request, jsonify
import pandas as pd
from rapidfuzz import fuzz, process
import base64, io, uuid, threading, os
from datetime import datetime

app = Flask(__name__)

# Load master well list once
MASTER_DF = pd.read_excel("MasterWells.xlsx")
MASTER_WELL_NAMES = MASTER_DF["Well Name"].dropna().astype(str).tolist()

# In-memory job store
jobs = {}

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

        jobs[job_id] = {
            "status": "done",
            "fileContent": encoded_result,
            "fileName": "matched_wells.xlsx",
            "created_at": datetime.utcnow().isoformat(),
            "total_wells": len(results),
            "matches_over_90": matches_over_90,
            "matches_below_90": matches_below_90,
            "percent_high_quality": percent_high_quality
        }

        print(f"[✅] Job {job_id} completed successfully.")
    except Exception as e:
        jobs[job_id] = {"status": "error", "message": str(e)}
        print(f"[❌] Job {job_id} failed with error: {e}")


# Submit endpoint
@app.route('/submit-task', methods=['POST'])
def submit_task():
    data = request.json

    if not data or 'file' not in data or 'well_column' not in data:
        return jsonify({"status": "error", "message": "Missing 'file' or 'well_column'"}), 400

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "pending",
        "created_at": datetime.utcnow().isoformat()  # <-- ADD THIS
    }

    # Start background thread
    thread = threading.Thread(target=process_file_async, args=(job_id, data['file'], data['well_column']))
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
    job_summaries = {}

    for job_id, job in jobs.items():
        job_summaries[job_id] = {
            "status": job["status"],
            "created_at": job.get("created_at"),
            "total_wells": job.get("total_wells"),
            "matches_over_90": job.get("matches_over_90"),
            "matches_below_90": job.get("matches_below_90"),
            "percent_high_quality": job.get("percent_high_quality")
        }

    return jsonify(job_summaries)


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


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
