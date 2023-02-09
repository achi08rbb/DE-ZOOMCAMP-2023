from pathlib import Path
import pandas as pd 
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials # another block made in orion ui

@task(retries=3) # from path
def extract_from_gcs(color:str,year:int,month:int) -> Path:
    """Download trip data from GCS"""
    path = Path(f"./data/{color}")
    path.mkdir(parents=True, exist_ok=True) # make the directory for the file first
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet" # this is the exact directory containing the target file
    gcs_block = GcsBucket.load("zoom-gcs", validate=False) # block is a prefect asset
    gcs_block.get_directory(from_path=gcs_path, local_path=f"") # used to download the directory gcs_path to local path, since we're running the script on the target directory, it will make the whole directory for us
    return Path(f"{gcs_path}")

@task() # path to df
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""

    df = pd.read_parquet(path) # The path containing our file will become a dataframe
    return df

@task()
def write_bq(df:pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp1.rides",
        project_id="prefect-zoomcamp",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(log_prints=True)
def etl_gcs_to_bq(color:str, year:int, month:int):
    """Main ETL flow to load data (from google cloud storage) into Big Query"""
    # Define all parameters for the tasks functions

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    print(f"rows:{len(df)}")

@flow(log_prints=True)
def etl_gcs_to_bq_parent(colors:list[str]=["green"], years:list[int]=[2020], months:list[int]=[1]):
    """Parent flow of the Main ETL Flow for Parametrization"""

    for color in colors:
        for month in months:
            for year in years:  
                etl_gcs_to_bq(color,year,month)

if __name__=="__main__":
    
    colors=["yellow"]
    years=[2019]
    months=[2,3]

    etl_gcs_to_bq_parent(colors, years, months)
