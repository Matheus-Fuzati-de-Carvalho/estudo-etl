from flask import Flask
import requests
import google.auth
from google.auth.transport.requests import AuthorizedSession

app = Flask(__name__)

# URLs dos seus Cloud Runs existentes
DRIVE_TO_GCS_URL = "https://SEU-DRIVE-TO-GCS.run.app"
GCS_TO_GBQ_URL = "https://SEU-GCS-TO-GBQ.run.app"

# Dataform
PROJECT_ID = "estudo-etl-481613"
LOCATION = "us-central1"
REPO = "dataform-estudo-etl"

@app.route("/", methods=["POST", "GET"])
def orchestrate_pipeline():

    # 1️⃣ Drive → GCS
    r1 = requests.post(DRIVE_TO_GCS_URL)
    r1.raise_for_status()

    # 2️⃣ GCS → BigQuery RAW
    r2 = requests.post(GCS_TO_GBQ_URL)
    r2.raise_for_status()

    # 3️⃣ Disparar Dataform
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    session = AuthorizedSession(creds)

    dataform_url = (
        f"https://dataform.googleapis.com/v1beta1/projects/{PROJECT_ID}"
        f"/locations/{LOCATION}/repositories/{REPO}/workflowInvocations"
    )

    body = {
        "compilationResult": f"projects/{PROJECT_ID}/locations/{LOCATION}"
                             f"/repositories/{REPO}/compilationResults/latest"
    }

    r3 = session.post(dataform_url, json=body)
    r3.raise_for_status()

    return "Pipeline completo executado com sucesso", 200


if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
