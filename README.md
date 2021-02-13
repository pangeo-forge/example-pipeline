# Example Pipeline

This repo includes an example pangeo-forge pipeline using the NOAA SST dataset.

## Run the example locally using conda and python

Pre-requisites:
* [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Create a local Zarr store:

```bash
conda env create -f=envirnoment.yml
conda activate pangeo-python-pipeline
python recipes/python-pipeline.py
```

Test the output in a python interpreter:

```python
>>> import os
>>> import xarray as xr
>>> ds = xr.open_zarr(f"{os.getcwd()}/recipe/noaa_sst.zarr")
```

## Run the example on Prefect Cloud

Pre-requisite:
* Create an account and project "pangeo-forge" on cloud.prefect.io
* Install prefect 

Login to prefect
Create + run a local agent
Run the workflow
```
prefect run flow --name "Rechunker" --project "pangeo-forge"
```