# Example Pipeline

This repo includes an example pangeo-forge pipeline using the NOAA SST dataset.

## Run the example locally using conda and python

Pre-requisites:
* [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Create a local Zarr store:

```bash
conda env create -f=envirnoment.yml
conda activate pangeo-pipeline
python recipes/pipeline.py
```

Test the output in a python interpreter:

```python
>>> import os
>>> import xarray as xr
>>> ds = xr.open_zarr(f"{os.getcwd()}/recipe/noaa_sst.zarr")
```

## Run the example on Prefect Cloud

Pre-requisite:
* Create an account on cloud.prefect.io
* Install prefect 

Authenticate with prefect, create the `pangeo-forge` project and start a local agent

```bash
export PREFECT_TOKEN=XXX
prefect auth login -t $PREFECT_TOKEN
export PREFECT__CLOUD__AGENT__AUTH_TOKEN=$(prefect auth create-token -n my-runner-token -s RUNNER)
prefect create project pangeo-forge
prefect agent local start
```

Run the workflow
```bash
prefect run flow --name "Rechunker" --project "pangeo-forge"
```