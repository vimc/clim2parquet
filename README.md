# clim2parquet: Convert climate data CSVs to Parquet

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/license/mit/)
[![PyPI - Version](https://img.shields.io/pypi/v/clim2parquet.svg)](https://pypi.org/project/clim2parquet)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/clim2parquet.svg)](https://pypi.org/project/clim2parquet)
[![codecov](https://codecov.io/gh/vimc/clim2parquet/graph/badge.svg?token=W5gs0PqZDu)](https://codecov.io/gh/vimc/clim2parquet)
[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)

_clim2parquet_ is a small Python package for internal use at [RESIDE-IC](https://reside-ic.github.io/) to convert prepared climate time-series data into Parquet files in support of climate-informed infectious disease modelling for [VIMC](https://www.vaccineimpact.org/).

## Installation

_clim2parquet_ can be installed from GitHub using a Python package manager.

```sh
pip install clim2parquet@git+https://github.com/vimc/clim2parquet
```

## Quick start

_clim2parquet_ is intended to be used internally with access to climate data generated at Imperial College.
With access to this data, you can convert country-specific climate data time-series by GADM admin level into Parquet files.

```python
import clim2parquet

# get available data sources
clim2parquet.get_data_names()

# with `dir_from` as your country-specific climate data source
# for GADM admin level 1 (largest sub-national unit)
clim2parquet.clim_to_parquet(
    data_source="CHIRPS", dir_from=dir_from, dir_to=".", admin_level=1
)

# converting multiple data sources and admin levels at once
clim2parquet.clim_to_parquet(
    data_source=["CHIRPS", "PERSIANN"], dir_from=dir_from,
    dir_to=".", admin_level=[0, 1]
)
```

We currently support finding and converting the following climate data sources:

- CHIRPS
- ERA5 mean temperature
- ERA5 maximum temperature
- ERA5 minimum temperature
- ERA5 relative humidity
- ERA5 specific humidity
- PERSIANN

## Help

To report a bug please [open an issue](https://github.com/vimc/clim2parquet/issues/new) or get in touch with RESIDE-IC.

## Contribute

Contribute [via a pull request](https://github.com/vimc/clim2parquet/pulls). 
