
# Admin unit indexing

_clim2parquet_ provides an index file for administrative units that aims to follow the GADM format, allowing users to link prepared climate data with GADM data.

The package data `admin_units_index` is a dataset with the following columns taken from GADM:

- "GID_0" (country ISO 3 character code);

- "GID_1", "GID_2", "GID_3" (sub-national unit identifiers);

- "NAME_0", "NAME_1", "NAME_2", "NAME_3" (country and sub-national unit names).

A further column "admin_unit_id" provides an integer identifier for each admin unit.

## Differences from GADM data

The admin unit index file **differs from GADM data**, and the column "admin_unit_id" *does not* correspond to the "UID" column found in GADM data.

There are extra entries (rows) for each admin level < 3, that is entries where for "GID_X" every "GID_(Y < X)" is missing.

In GADM data, each entry is resolved down to the lowest available admin level (which may be as low as level 5 for some countries), with a unique identifier "UID" for each one.

Since we resolve data down to multiple levels of interest (up to level 3), we have had to construct a separate identifier that is applicable only to climate data generated using `clim_to_parquet`.

You will need the package admin unit index file in order to link prepared climate data with GADM data.

## Preparing the index file

The index file is prepared from [GADM spatial data](https://gadm.org/download_world.html). See and run the script `data-raw/admin_unit_ids.py` to generate data. The recommended way of doing this is using the [`uv` package manager](https://docs.astral.sh/uv/).

```sh
uv run data-raw/admin_unit_ids.py
```
