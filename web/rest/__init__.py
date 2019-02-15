from crypt import methods

from flask import Flask, request, render_template
app = Flask(__name__)
import pyarrow.parquet as pq



@app.route("/")
def index():
    return  render_template("index.html")

@app.route("/anomaly")
def anomaly():
    # TODO: fetch anomaly by document_id
    id = request.args.get('_ID')
    if id is None:
        return "Must provide LAD_ID"

    return "anomaly"


def persistMetadataToFactStore(id, anomaly):
    # TODO: Store results in parquet store
    pass


@ app.route("/feedback", methods=['GET'])
def feedback():
    # TODO: persist to parquet file invalid anomaly
    id = request.args('LAD_ID')
    anomaly = request.values.get('anomaly')

    if id is None or anomaly is None:
        return "Must provide LAD_ID or Anomaly parameter"
    persistMetadataToFactStore(id,anomaly)
    # TODO: Redirect user to confirmation page that we have logged the false anomaly.
    return "feedback+ "+id


