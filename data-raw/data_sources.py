# SPDX-FileCopyrightText: 2025-present Imperial College of Science and Technology
#
# SPDX-License-Identifier: MIT

"""
Generate package data.

Generate package data file `data_sources.csv` which holds data on the
climate data sources.
"""

from pathlib import Path

import pandas as pd

data_path = Path("src/clim2parquet/data")
file_name = "data_sources.csv"
data_path = data_path / file_name

# create a DataFrame with the data sources
data_source = pd.Series(
    [
        "CHIRPS",
        "ERA5_mean",
        "ERA5_min",
        "ERA5_max",
        "ERA5_RH",
        "ERA5_SH",
        "PERSIANN",
    ]
)

# data source versions currently unknown
data_version = pd.Series(["" for i in data_source])

data_regex = pd.Series(
    [
        "CHIRPS",
        "ERA5_Land_2m_temperature_\\d{4}",
        "ERA5_Land_2m_temperature_daymin",
        "ERA5_Land_2m_temperature_daymax",
        "ERA5_Land_relative_humidity",
        "ERA5_Land_specific_humidity",
        "PERSIANN",
    ]
)

data_output_name = pd.Series(
    ["chirps", "era5mean", "era5min", "era5max", "era5rh", "era5sh", "persiann"]
)

data = pd.DataFrame(
    {
        "data_source": data_source,
        "data_version": data_version,
        "data_regex": data_regex,
        "data_output_name": data_output_name,
    }
)

# write package to location --- has to be done manually
data.to_csv(data_path, index=False)
