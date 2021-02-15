# Example Pipeline

This repo includes an example pangeo-forge pipeline using the NOAA SST dataset.

## Run the example locally using conda and python

Pre-requisites:
* [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Create a local Zarr store:

```bash
conda env create -f=environment.yml
conda activate pangeo-pipeline
python recipes/pipeline.py --storage 'file://recipe/noaa_sst.zarr'
```

Test the output in a python interpreter:

```python
>>> import os
>>> import xarray as xr
>>> ds = xr.open_zarr(f"{os.getcwd()}/recipe/noaa_sst.zarr")
```

## Run the example on Prefect Cloud

Pre-requisites:
* Create an account on [cloud.prefect.io](cloud.prefect.io)
* AWS account and credentials for S3 read/write access

Authenticate with prefect, create the `pangeo-forge` project and start a local agent:

```bash
export PREFECT_TOKEN=XXX
prefect auth login -t $PREFECT_TOKEN
export PREFECT__CLOUD__AGENT__AUTH_TOKEN=$(prefect auth create-token -n my-runner-token -s RUNNER)
prefect create project pangeo-forge
prefect agent local start
```

Configure S3 Access:

```bash
cp recipe/config.yml.example recipe/config.yml
```

Update the values in `config.yml` for prefect runners to write to a remote cache and target.

Finally run the workflow:

```bash
python recipe/pipeline.py --execution_env 'prefect' --storage 's3://YOUR_BUCKET/YOUR_PATH/noaa_sst.zarr'
```

## Github Workflows

NOTE: The scripts and workflows in `.github/` are out of date with the current example code.
