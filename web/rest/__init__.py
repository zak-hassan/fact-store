from flask import Flask, request
app = Flask(__name__)

@app.route("/")
def index():
    return "Healthz"

@app.route("/anomaly")
def anomaly():
    # TODO: fetch anomaly by document_id
    id = request.args.get('_ID')
    if id is None:
        return "Must provide LAD_ID"

    return "anomaly"


def persistMetadataToFactStore(id):
    pass


@ app.route("/feedback")
def feedback():
    # TODO: persist to parquet file invalid anomaly
    id = request.args.get('LAD_ID')
    if id is None:
        return "Must provide LAD_ID"
    persistMetadataToFactStore(id)
    return "feedback+ "+id


