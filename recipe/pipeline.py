# Standard library imports.
import argparse
import os
import shutil
import s3fs
import tempfile
import yaml

# Related third party imports.
import fsspec
from fsspec.implementations.local import LocalFileSystem
import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe
from pangeo_forge.storage import CacheFSSpecTarget, FSSpecTarget
from pangeo_forge.executors import PythonPipelineExecutor, PrefectPipelineExecutor
from prefect import task, Flow

parser = argparse.ArgumentParser()
parser.add_argument('--execution_env', help='Optional argument to run a flow remotely, such as on prefect cloud')
parser.add_argument('--storage', help='Optional argument to store data remotely, such as on AWS')
args = parser.parse_args()

# NOAA SST Specific Functions for generating a list of URLs
def get_source_url(day: str) -> str:
    """
    Format the URL for a specific day.
    """
    day = pd.Timestamp(day)
    source_url_pattern = (
        "https://www.ncei.noaa.gov/data/"
        "sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/"
        "{day:%Y%m}/oisst-avhrr-v02r01.{day:%Y%m%d}.nc"
    )
    return source_url_pattern.format(day=day)

days = pd.date_range("1981-09-01", "1981-09-10", freq="D").strftime("%Y-%m-%d").tolist()
source_urls = [get_source_url(day) for day in days]

# Create the pangeo-forge recipe
recipe = NetCDFtoZarrSequentialRecipe(
    input_urls=source_urls,
    sequence_dim="time"
)
this_dir = os.path.dirname(os.path.abspath(__file__))

storage_protocol = args.storage.split(':')[0]
fs = fsspec.get_filesystem_class(storage_protocol)()

if fs.__class__.__name__ == 'S3FileSystem':
    with open(f"{this_dir}/config.yml") as config_file:
        config = yaml.safe_load(config_file)
    fs.key=config['MY_AWS_KEY']
    fs.secret=config['MY_AWS_SECRET']
    # REVIEW: Where should cache be stored in S3FileSystem? Should we create a temporary directory?
    cache_path=f"{args.storage}/cache"

if fs.__class__.__name__ == 'LocalFileSystem':
    cache_dir = tempfile.TemporaryDirectory()
    cache_path = cache_dir.name

target = FSSpecTarget(fs, root_path=args.storage)
cache_target = CacheFSSpecTarget(fs=fs, root_path=cache_path)
recipe.input_cache = cache_target
recipe.target = target
pipeline = recipe.to_pipelines()

if args.execution_env == 'prefect':
    executor = PrefectPipelineExecutor()
    plan = executor.pipelines_to_plan(pipeline)
    plan.register(project_name="pangeo-forge")
    # plan.run() <-- This will run the plan on a local system. To run on a CloudFlowRunner, must use the following prefect CLI command:
    os.system("prefect run flow --name \"Rechunker\" --project \"pangeo-forge\"")
else:
    executor = PythonPipelineExecutor()
    plan = executor.pipelines_to_plan(pipeline)
    executor.execute_plan(plan)
