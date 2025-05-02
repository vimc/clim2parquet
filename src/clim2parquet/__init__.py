# SPDX-FileCopyrightText: 2025-present Imperial College of Science and Technology
#
# SPDX-License-Identifier: MIT

"""
Convert country climate data to Parquet files.

This package provides functionality to find and convert country climate data,
assumed to be stored as CSVs, to Parquet files.

This package contains the functions:

- `get_data_names()`  -  Returns names of climate data sources.
- `clim_to_parquet()`  -  Converts climate data to a single Parquet file.
"""

from pathlib import Path
from typing import Optional

from clim2parquet import tools


def get_data_names() -> list[str]:
    """
    Get data source names.

    Returns
    -------
    list[str]
        A list of data source names.
    """
    return tools._data_source_info()["data_source"].tolist()


def clim_to_parquet(  # noqa: C901
    data_source: str | list[str],
    dir_from: str | Path,
    dir_to: str | Path,
    admin_level: Optional[int] | Optional[list[int]] = None,
    gadm_version: str = "v410",
) -> None:
    """
    Convert country climate data to a Parquet file.

    Parameters
    ----------
    data_source: str, list[str]
        A data source or a list of data sources. See available data sources in
        `get_data_names()`.
    dir_from : str, Path
        Path to the CSV data directory, as a `str` or `pathlib.Path`.
    dir_to: str, Path
        Path to the output directory where Parquet files will be saved, as a
        `str` or `pathlib.Path`.
    admin_level : int, list[int], optional
        GADM admin level as an integer or a list of integers. May have values
        in the range 0 -- 3. Defaults to 0 indicating country level data.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0. No other
        versions are currently supported.

    Returns
    -------
    None
        Called for the side effect of converting CSV data files to Parquet files
        with each admin level combined into a single file where each admin-unit
        file is vertically concatenated. Existing variables are maintained.

        Columns `admin_unit_*` and `gid_code_version` are added for the numeric
        identifier for each admin level `*` and the GADM identification
        code version (usually `"1"`).

        E.g. Where admin-level 1 is `10` and admin-level 2 is `3`, the columns
        `admin_unit_1` and `admin_unit_2` will hold the values `"10"` and `"3"`
        respectively.

        The special case of country level data is handled by including a column
        `admin_unit_0` with all values set to `"0"`.


    Raises
    ------
    ValueError
        If `data_source`, `admin_level`, or `gadm_version` are not valid.
    Exception
        If `dir_from` or `dir_to` are not valid directories.

    Warns
    -----
    UserWarning
        If no climate files are found in the input directory.
    """
    # convert inputs to lists
    if admin_level is None:
        admin_level = [0]
    if isinstance(admin_level, int):
        admin_level = [admin_level]
    if isinstance(data_source, str):
        data_source = [data_source]

    # input checking
    if not all(d in get_data_names() for d in data_source):
        err_bad_clim = "One or more of `data_source` are not available. \
            Run `get_data_names()` to get available data names."
        raise ValueError(err_bad_clim)

    if not all(i in tools._gadm_levels() for i in admin_level):
        err_bad_admin = "One or more of `admin_level` are not available. \
            Supported levels are: 0, 1, 2, 3."
        raise ValueError(err_bad_admin)

    if gadm_version not in tools._gadm_versions():
        err_bad_gadm = "GADM version not available. Only version 4.1.0 is\
            supported, and is specified as 'v410'."
        raise ValueError(err_bad_gadm)

    path_dir_from = Path(dir_from)
    if not path_dir_from.is_dir():
        err_no_from = f"Data source directory {dir_from} not found or is not \
            a directory."
        raise Exception(err_no_from)

    path_dir_to = Path(dir_to)
    if not path_dir_to.is_dir():
        err_no_dest = f"Data output directory {dir_to} not found or is not \
            a directory."
        raise Exception(err_no_dest)

    # load or make admin unit UIDs
    # suppress warnings
    # check if admin unit table is present and generate if not
    admin_unit_ids = tools._data_admin_unit_ids()

    # currently offering only data source and admin level combinations
    for d in data_source:
        for i in admin_level:
            # NOTE: file finder fun throws a warning if no files are found
            in_files = tools._find_clim_files(path_dir_from, d, i, gadm_version)
            if len(in_files) < 1:
                break
            else:
                # out_file is a string as pyarrow does not support pathlib.Path
                out_file = str(path_dir_to / tools._make_output_names(d, i))
                tools._files_to_parquet(
                    in_files, out_file, i, admin_unit_ids, gadm_version
                )
