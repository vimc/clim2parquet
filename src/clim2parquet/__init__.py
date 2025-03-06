# SPDX-FileCopyrightText: 2025-present Imperial College of Science and Technology
#
# SPDX-License-Identifier: MIT

from . import tools

import os
from typing import Union
import warnings


def get_data_names():
    """
    Get data source names.

    Returns
    -------
    list
        A list of data source names.
    """
    return tools._get_data_info()["data_source"].tolist()


def clim_to_parquet(
    data_source: Union[str, list],
    dir_from: str,
    dir_to: str,
    admin_level: Union[int, list] = [0],
    gadm_version: str = "v410",
):
    """
    Convert country climate data to a Parquet file.

    Parameters
    ----------
    data_source: str, list
        A data source or a list of data sources. See available data sources in
        `get_data_names()`.
    dir_from : str
        Path to the CSV data directory.
    dir_to: str
        Path to the output directory where Parquet files will be saved.
    admin_level : list
        GADM admin level as an integer or a list of integers. May have values
        in the range 0 -- 3.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0. No other
        versions are currently supported.

    Returns
    -------
    None. Called only for the side effect of converting CSV data files to
    Parquet.
    """

    # convert inputs to lists
    if isinstance(admin_level, int):
        admin_level = [admin_level]

    if isinstance(data_source, str):
        data_source = [data_source]

    # input checking
    if not all([d in get_data_names() for d in data_source]):
        raise ValueError(
            "One or more of `data_source` are not available. \
            Run `get_data_names()` to get available data names."
        )

    if not all([i in tools._gadm_levels() for i in admin_level]):
        raise ValueError(
            "One or more of `admin_level` are not available. \
            Supported levels are: 0, 1, 2, 3."
        )

    if gadm_version not in tools._gadm_versions():
        raise ValueError(
            "GADM version not available. Only version 4.1.0 is\
            supported, and is specified as 'v410'."
        )

    if not os.path.exists(dir_from):
        raise Exception("Data source directory `dir_from` not found.")

    if not os.path.exists(dir_to):
        raise Exception("Data output directory `dir_to` not found.")

    # currently offering only data source and admin level combinations
    for d in data_source:
        for i in admin_level:
            # NOTE: file finder fun throws a warning if no files are found
            in_files = tools._find_clim_files(dir_from, d, i, gadm_version)
            if len(in_files) < 1:
                break
            else:
                out_file = os.path.join(dir_to, tools._make_output_names(d, i))
                tools._files_to_parquet(in_files, out_file)
