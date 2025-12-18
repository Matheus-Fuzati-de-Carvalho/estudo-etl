from flask import Flask
from google.cloud import bigquery
import os

app = Flask(__name__)

PROJECT_ID = "estudo-etl-481613"
DATASET_ID = "raw"
TABLE_ID = "raw_tabelas"
GCS_URI = "gs://estudo_etl/raw/tabelas/tabelas.csv"

@app.route("/", methods=["GET", "POST"])
def load_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = client.load_table_from_uri(
        GCS_URI,
        table_ref,
        job_config=job_config
    )

    load_job.result()

    return "Carga para BigQuery RAW concluída com sucesso", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    
