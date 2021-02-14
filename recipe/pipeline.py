import argparse
from fsspec.implementations.local import LocalFileSystem
import os
import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe
from pangeo_forge.storage import CacheFSSpecTarget, FSSpecTarget
from pangeo_forge.executors import PythonPipelineExecutor, PrefectPipelineExecutor
import shutil
import s3fs
import tempfile
import prefect
from prefect import task, Flow

def source_url(day: str) -> str:
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
sources = list(map(source_url, days))

recipe = NetCDFtoZarrSequentialRecipe(
    input_urls=sources,
    sequence_dim="time"
)

parser = argparse.ArgumentParser()
parser.add_argument('--execution_env', help='Optional argument to run a flow remotely, such as on prefect cloud')
parser.add_argument('--storage', help='Optional argument to store data remotely, such as on AWS')
args = parser.parse_args()

config = {
    'MY_AWS_KEY': 'XXX',
    'MY_AWS_SECRET': 'XXX',
    's3': {
        'target_bucket': 'aimee-tf-state',
        'target_path': 'pangeo-forge/target',
        'cache_bucket': 'aimee-tf-state',
        'cache_path': 'pangeo-forge/cache'
    }
}

if args.storage == 's3':
    fs = s3fs.S3FileSystem(key=config['MY_AWS_KEY'], secret=config['MY_AWS_SECRET'])
    target_path = f"{config['s3']['target_bucket']}/{config['s3']['target_path']}/noaa_sst.zarr"
    cache_path = f"{config['s3']['cache_bucket']}/{config['s3']['cache_path']}"
else:
    fs = LocalFileSystem()
    this_dir = os.path.dirname(os.path.abspath(__file__))
    target_path = os.path.join(this_dir, 'noaa_sst.zarr')
    if os.path.exists(target_path):
        shutil.rmtree(target_path)
    os.mkdir(target_path)
    cache_dir = tempfile.TemporaryDirectory()
    cache_path = cache_dir.name

target = FSSpecTarget(fs, root_path=target_path)
cache_target = CacheFSSpecTarget(fs=fs, root_path=cache_path)
recipe.input_cache = cache_target
recipe.target = target
pipeline = recipe.to_pipelines()

if args.execution_env == 'prefect':
    executor = PrefectPipelineExecutor()
    print(executor)
    plan = executor.pipelines_to_plan(pipeline)
    # The 'plan' a prefect flow
    plan.register(project_name="pangeo-forge")
    plan.run(executor=executor)
else:
    executor = PythonPipelineExecutor()
    plan = executor.pipelines_to_plan(pipeline)
    executor.execute_plan(plan)

# executor.execute_plan(plan)
# flow.run() We could run our flow locally using the flow's run method but we'll be running this from Cloud!
# Right now this fails because the files aren't being downloaded, need to look at https://pangeo-forge.readthedocs.io/en/latest/recipes.html#storage
# `prefect run flow --name "Rechunker" --project "pangeo-forge"`