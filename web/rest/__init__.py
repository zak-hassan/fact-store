import pandas as pd
from fastparquet import write
from flask import Flask, request, render_template
app = Flask(__name__, static_folder="static")
import s3fs
import os
import pyarrow.parquet as pq

import pyarrow as pa


@app.route("/")
def index():
    return  render_template("index.html")



def persistMetadataToFactStore(id, anomaly):
    # TODO: Store results in parquet store
    s3_key = os.getenv("LAD_S3_KEY")
    s3_secret = os.getenv("LAD_S3_SECRET")
    s3_host = os.getenv("LAD_S3_HOST")
    s3_bucket= os.getenv("LAD_S3_BUCKET")
    client_kwargs =  {'endpoint_url': s3_host}
    s3 = s3fs.S3FileSystem(key=s3_key,
                           secret=s3_secret,
                           client_kwargs=client_kwargs)
    myopen = s3.open(s3_bucket+"/factstore.parquet","wb")
    df = pd.DataFrame({'_id': [id], 'is_false_anomaly': [anomaly]})
    ta = pa.Table.from_pandas(df)
    pw = pq.ParquetWriter(myopen, schema=ta.schema)
    pw.write_table(ta)
    pw.close()



@app.route("/api/feedback", methods=['GET'])
def feedback():
    id = request.args.get('lad_id')
    anomaly = request.args.get('false_anomaly')
    if id is None or anomaly is None:
        return "Must provide lad_id or anomaly parameter"
    print("id: "+ id);
    print("anomaly: "+ anomaly);
    persistMetadataToFactStore(id,anomaly)
    return "success"


