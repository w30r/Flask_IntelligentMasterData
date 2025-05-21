from flask import Flask, request, jsonify
import pandas as pd
from rapidfuzz import fuzz, process
import base64, io, uuid, threading, os

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

        jobs[job_id] = {
            "status": "done",
            "fileContent": encoded_result,
            "fileName": "matched_wells.xlsx"
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
    jobs[job_id] = {"status": "pending"}

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
    return jsonify({job_id: {"status": j["status"], "created_at": j.get("created_at")} for job_id, j in jobs.items()})


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
