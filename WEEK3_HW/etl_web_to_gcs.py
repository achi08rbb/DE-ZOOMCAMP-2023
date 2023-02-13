from pathlib import Path
import pandas as pd 
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url:str): 
    """"Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    # df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime']) # if green data
    # df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    #df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime']) # for yellow data
    #df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime']) # for fhv data
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
    print(df.head(2))
    print(f"columns:{df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    path = Path(f"data/")
    path.mkdir(parents=True, exist_ok=True) # make the directory for the file first, if directory now exits use exist_ok=True, so that the function will continue
    path2 = Path(f"data/{dataset_file}.csv.gz") # this makes the file
    df.to_csv(path2, compression="gzip")
    return path2

@task(log_prints=True)
def write_gcs(path: Path, dataset_file: str) -> None: # None means this function returns nothing
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=f"data/fhv/{dataset_file}.csv.gz") # create a string with no Path() to make the actual directory # must spell out the path in gcs to create folders not just the file
    return
#task 

@flow()
def etl_web_to_gcs(year:int, month:int) -> None: 
    """The main ETL function"""

    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path, dataset_file)

@flow()
def etl_web_to_gcs_parent(years:list[int], months:list[int]):
    """Parent of the main ETL function for Parameterization"""
    for month in months:
        for year in years:  
            etl_web_to_gcs(year,month)

if __name__=='__main__': 
# this function will only run if we want it to
    years = [2019]
    # months = list(range(1,13))
    months = list(range(1,13))
    etl_web_to_gcs_parent(years,months)