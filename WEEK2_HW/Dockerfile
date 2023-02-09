FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY etl_web_to_gcs.py /opt/prefect/WEEK2_HW/etl_web_to_gcs.py 
COPY data /opt/prefect/data
