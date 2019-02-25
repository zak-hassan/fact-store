from flask import Flask, request, render_template
import pandas as pd
from fastparquet import write, ParquetFile
import boto3
import os
import datetime

PARQUET_FILE_NAME = 'factstore.parquet'
app = Flask(__name__, static_folder="static")



@app.route("/")
def index():
    return  render_template("index.html")

def syncModel():
    """
    Will check if the after training you would like to store models in s3
    with timestamp. It will follow a particular pattern:

    s3://<bucket_name>/<path>/<timestamp>

    Later after running training you can serve models from a particular timestamp.

    :return:
    """

    session = boto3.session.Session()
    s3_key = os.getenv("LAD_S3_KEY")
    s3_secret = os.getenv("LAD_S3_SECRET")
    s3_host = os.getenv("LAD_S3_HOST")
    s3_client = session.client(service_name='s3',
                               aws_access_key_id=s3_key,
                               aws_secret_access_key=s3_secret,
                               endpoint_url=s3_host)

    #s3_bucket = os.getenv("LAD_S3_BUCKET")

    s3_client.upload_file(Filename="factstore.parquet", Bucket="AIOPS", Key="anomaly-detection/metadata/factstore.parquet")
    print.info("Done uploading models to s3 complete")

def persistMetadataToFactStore(id, anomaly):
    # TODO: Store results in parquet store
    df = pd.DataFrame({'id': [id], 'is_false_anomaly': [anomaly],'date': [datetime.datetime.now()]})
    if os.path.exists(PARQUET_FILE_NAME):
        write(filename=PARQUET_FILE_NAME, data=df, append=True, compression='GZIP')
    else:
        write(filename=PARQUET_FILE_NAME, data=df, compression='GZIP')
    print("Syncing factstore")
    # TODO: Uncomment below so you can test out S3 integration.
    #syncModel()



@app.route("/api/metadata", methods=['GET'])
def metadata():
    """ Service to provide list of false anomalies to be relabeled during ml training run"""
    pf = ParquetFile('factstore.parquet')
    df = pf.to_pandas()
    return df.to_json(orient='records')

@app.route("/api/feedback", methods=['GET'])
def feedback():
    """ Feedback Service to provide user input on which false predictions this model provided."""
    id = request.args.get('lad_id')
    anomaly = request.args.get('false_anomaly')
    if id is None or anomaly is None:
        return "Must provide lad_id or anomaly parameter"
    print("id: "+ id);
    print("anomaly: "+ anomaly);
    persistMetadataToFactStore(id,anomaly)
    return "success"


