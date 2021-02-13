from fsspec.implementations.local import LocalFileSystem
import os
import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe
from pangeo_forge.storage import CacheFSSpecTarget, FSSpecTarget
from pangeo_forge.executors import PythonPipelineExecutor, PrefectPipelineExecutor
import shutil
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

fs_local = LocalFileSystem()

cache_dir = tempfile.TemporaryDirectory()
cache_target = CacheFSSpecTarget(fs_local, cache_dir.name)

this_dir = os.path.dirname(os.path.abspath(__file__))
target_dir_name = os.path.join(this_dir, 'noaa_sst.zarr')
if os.path.exists(target_dir_name):
    shutil.rmtree(target_dir_name)
os.mkdir(target_dir_name)
target = FSSpecTarget(fs_local, target_dir_name)

recipe.input_cache = cache_target
recipe.target = target
pipeline = recipe.to_pipelines()
# executor = PythonPipelineExecutor()
executor = PrefectPipelineExecutor()
# This is also a prefect flow
plan = executor.pipelines_to_plan(pipeline)
print(plan)
plan.register(project_name="pangeo-forge")
# executor.execute_plan(plan)
# flow.run() We could run our flow locally using the flow's run method but we'll be running this from Cloud!
# Right now this fails because the files aren't being downloaded, need to look at https://pangeo-forge.readthedocs.io/en/latest/recipes.html#storage
# `prefect run flow --name "Rechunker" --project "pangeo-forge"`
