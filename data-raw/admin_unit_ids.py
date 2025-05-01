# SPDX-FileCopyrightText: 2025-present Imperial College of Science and Technology
#
# SPDX-License-Identifier: MIT

"""
Generate admin unit identifiers and country codes as package data.

Generate package data file `admin_units.csv` which holds data on admin units
and acts as an index of country-specific identifiers of admin units based on
GADM data.

Also generate package data file `country_codes.csv` which holds ISO 3 character
country codes for faster access.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd  # to write CSV to file

# NOTE: assume the CWD is top-level dir

source_path = Path("data-raw/data-spatial")
source_file = "gadm_410.gpkg"  # Not included in this pkg as too large
source_file = source_path / source_file

dest_path = Path("src/clim2parquet/data")
file_name = "admin_units.csv"
dest_file = dest_path / file_name

# check layers; expected 1 layer for all admin levels in this dataset
# read in file, drop geometry and work with Pandas DF
gpd.list_layers(source_file)
data_gpd = gpd.read_file(source_file)
data_pd = pd.DataFrame(data_gpd.drop(columns="geometry"))

# select up to lowest (larger = lower) level of interest and get
# unique values
admin_levels_wanted = [0, 1, 2, 3]
gid_colnames = [f"GID_{i}" for i in admin_levels_wanted]
readable_colnames = [f"NAME_{i}" for i in admin_levels_wanted]
colnames = [*gid_colnames, *readable_colnames]

data_pd = data_pd[colnames]
data_pd = data_pd.drop_duplicates()

# prepare entries for top level admin units as GADM does not include these
# i.e., for country-level, admin 1-level, and admin 2-level
top_gadm_levels = [0, 1, 2]  # NOTE: not level 3

data_list: list[pd.DataFrame] = []
for i in top_gadm_levels:
    gid_names = [f"GID_{j}" for j in list(range(i + 1))]
    readable_names = [f"NAME_{j}" for j in list(range(i + 1))]

    df = data_pd[gid_names + readable_names].drop_duplicates()
    data_list.append(df)

# NOTE: not sorting by GID; accepting current sorting
data = pd.concat(data_list, ignore_index=True)
data = pd.concat([data, data_pd])
data = data[colnames]

data = data.reset_index(drop=True)

# assign ids and save
data["admin_unit_id"] = range(len(data))
data.to_csv(dest_file, index=False)

## save country codes for easy access ##
dest_file = dest_path / "country_codes.csv"
country_codes = data["GID_0"].drop_duplicates()
country_codes.to_csv(dest_file)
