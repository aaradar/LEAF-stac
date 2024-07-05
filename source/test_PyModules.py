import json
import time
import logging
from typing import Any

import click

#from auth import auth

import eoMosaic as eoMZ


# init logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(params):
    #from lib import LEAFNets as LFNs

    task_list = eoMZ.period_mosaic(params)    
    while any([task.active() for task in task_list]):
        # count active tasks
        for task in task_list:
            if task.active():
                logger.info(f"The task {task.id} is active.")
            else:
                logger.info(f"The task {task.id} is not active.")

        logger.info("-" * 50)
        time.sleep(30)



@click.command()
@click.option(
    "--sensor",
    help="A string indicating sensor type and data unit (e.g., 'S2_SR' or 'L8_SR')",
    required=True,
)
@click.option(
    "--year", help="An integer representing image acquisition year", 
    required=True
)
@click.option(
    "--months",
    help="A set of integers representing months of the year (a negative value means a peak season from Jun.15 to Sept.15)",
    required=True,
)
@click.option(
    "--prod_names",
    help="A subset or all of the elements in ['LAI','fAPAR','fCOVER','Albedo', 'date', 'partition']",
    required=True,
)
@click.option(
    "--tile_names",
    help="A list of tile name strings as per the CCRS' tile griding system",
    required=True,
)
@click.option(
    "--resolution",
    help="The spatial resultion (in meter) of the exported products",
    required=True,
)
@click.option(
    "--out_location",
    help="The destination of exporting the products ('drive' or 'storage')",
    required=False,
)
@click.option(
    "--gcs_bucket",
    help="An unique bucket name on Google Cloud Storage (must have been created beforehand)",
    required=False,
)
@click.option(
    "--out_folder",
    help="The folder name intended for exporting products on either Google Drive or Google Cloud Storage",
    required=True,
)
@click.option(
    "--custom_region",
    help="A given user-defined region. Only include this 'key:value' pair as necessary",
    required=False,
)
@click.option(
    "--start_date",
    help="A string to specify the start date of a customized compositing period",
    required=False,
)
@click.option(
    "--end_date",
    help="A string to specify the end date of a customized compositing period",
    required=False,
)
@click.option(
    "--projection",
    help="A string representing a customized projection. Default projection = 'EPSG:3979'",
    required=False,
)



# params = {
#     'sensor': 'L8_SR',           # A sensor type string (e.g., 'S2_SR' or 'L8_SR' or 'MOD_SR')
#     'unit': 2,                   # A data unit code (1 or 2 for TOA or surface reflectance)    
#     'year': 2022,                # An integer representing image acquisition year
#     'nbYears': 1,                # positive int for annual product, or negative int for monthly product
#     'months': [8],               # A list of integers represening one or multiple monthes     
#     'tile_names': ['tile42_922'],   # A list of (sub-)tile names (defined using CCRS' tile griding system) 
#     'prod_names': ['mosaic'],    #['mosaic', 'LAI', 'fCOVER', ]    
#     'resolution': 1000,          # Exporting spatial resolution    
#     'out_folder': 'C:/Work_documents/test_xr_output',   # the folder name for exporting   
#     'CloudScore': True,

#     #'start_date': '2022-06-15',
#     #'end_date': '2023-09-15'
# }

def main(
    sensor: str,
    year: str,
    months: str,
    prod_names: str,
    tile_names: str,
    resolution: str,
    out_location: str,
    gcs_bucket: str,
    out_folder: str,
    custom_region: str,
    start_date: str,
    end_date: str,
    projection: str,
) -> Any:
    #auth.init_auth()

    params = {
        "sensor": sensor,
        "year": int(year),
        "months": [int(month) for month in json.loads(months)],
        "prod_names": json.loads(prod_names),
        "tile_names": json.loads(tile_names),
        "resolution": int(resolution),
        "out_location": out_location,
        "GCS_bucket": gcs_bucket,
        "out_folder": out_folder,
        "custom_region": json.loads(custom_region),
        "start_date": start_date,
        "end_date": end_date,
        "projection": projection,
    }
    
    print('all parameters = ', params)
    logger.info(params)

    # convert geometry to ee.Geometry
    #if "custom_region" in params:
    #    params["custom_region"] = ee.Geometry.Polygon(params["custom_region"])

    # DEBUG workflow
    # return
    run(params)



if __name__ == "__main__":
    main()




{"access_token":"eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJaRDV1MDdWZnoyOWtXd1hXYWozT1ZHLS1HUEFGcmcwVV94RzgwWG1Ja3dZIn0.eyJleHAiOjE3MTg0MTkyNDEsImlhdCI6MTcxODQxNzQ0MSwianRpIjoiYTIyOWVjZWYtMTdhYy00ZTA4LWI4Y2MtZGM3ZGZkNzczNzg2IiwiaXNzIjoiaHR0cHM6Ly9rZXljbG9hay5jc2FkZXYuZGVjLmVhcnRoZGFpbHkuY29tL3JlYWxtcy9tYXN0ZXIiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiMDkxYWJlZDEtNGY1NC00YTU3LWFhOTMtMzQ5NjhmMzY5NzQxIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoid3MtbGVhZiIsInNlc3Npb25fc3RhdGUiOiJjYTU4YzRlYS02Y2NlLTQ5MjMtOGQxNS1lM2E3ZGEwMWYwNmIiLCJhY3IiOiIxIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoicHJvZmlsZSBlbWFpbCIsInNpZCI6ImNhNThjNGVhLTZjY2UtNDkyMy04ZDE1LWUzYTdkYTAxZjA2YiIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwicHJlZmVycmVkX3VzZXJuYW1lIjoibGVhZiIsImdpdmVuX25hbWUiOiIiLCJmYW1pbHlfbmFtZSI6IiJ9.aYWlAvr51l9i3cVGqMHEeEEHSCicBuGtH47gz59jQlnwgarMT0a2WWcV4oMcbI84dV8-CDbYmoCr50MTyKSxp6ALr84n1CyPpuViDRufYceo7xCQqoc72NNsxDjjRCUUf8QOCLr6FI4nJBDQsmzbiZE45wnk8d2Z-XMMsu2mIyz0TncaMHdb8Cfsy5Ds-ldF7xzRinByCnPe4PjaxSrzENR9aLvdHLHaB82ZUMdRELu12pl2ilfJK32CCnPGfW5JpgdG_so_l7c7AHdJi31bL5Xs7nayWZIdpuO318hcpx8qpP8S0CeEdFGMVZJR7132MKOorKLF2mG_KtObsLl5gg",
 "expires_in":1800,
 "refresh_expires_in":1800,
 "refresh_token":"eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIyZjIwYjBmMS05NzIyLTQ2YzUtYTJjYy1mZTY2MDljMzU0Y2IifQ.eyJleHAiOjE3MTg0MTkyNDEsImlhdCI6MTcxODQxNzQ0MSwianRpIjoiODRkYjc3OWEtY2E4NS00MmVkLTllNDgtYzdlMTE1MjY1NGE0IiwiaXNzIjoiaHR0cHM6Ly9rZXljbG9hay5jc2FkZXYuZGVjLmVhcnRoZGFpbHkuY29tL3JlYWxtcy9tYXN0ZXIiLCJhdWQiOiJodHRwczovL2tleWNsb2FrLmNzYWRldi5kZWMuZWFydGhkYWlseS5jb20vcmVhbG1zL21hc3RlciIsInN1YiI6IjA5MWFiZWQxLTRmNTQtNGE1Ny1hYTkzLTM0OTY4ZjM2OTc0MSIsInR5cCI6IlJlZnJlc2giLCJhenAiOiJ3cy1sZWFmIiwic2Vzc2lvbl9zdGF0ZSI6ImNhNThjNGVhLTZjY2UtNDkyMy04ZDE1LWUzYTdkYTAxZjA2YiIsInNjb3BlIjoicHJvZmlsZSBlbWFpbCIsInNpZCI6ImNhNThjNGVhLTZjY2UtNDkyMy04ZDE1LWUzYTdkYTAxZjA2YiJ9.K01fA5fuQIz9t36Own5P4Ncs1pwKXBwvMEyUu1oVxmM",
 "token_type":"Bearer",
 "not-before-policy":0,
 "session_state":"ca58c4ea-6cce-4923-8d15-e3a7da01f06b",
 "scope":"profile email"}
