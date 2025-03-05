# SPDX-FileCopyrightText: 2025-present Imperial College of Science and Technology
#
# SPDX-License-Identifier: MIT

from . import tools

import os
from typing import Union


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
    dir_from : str
        Path to the data directory.
    dir_to: str
        Path to the output directory.
    admin_level : list
        GADM admin level as an integer or a list of integers.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """

    # convert inputs to lists
    if isinstance(admin_level, int):
        admin_level = [admin_level]

    if isinstance(data_source, str):
        data_source = [data_source]

    # input checking
    if not all([d in tools._get_data_names() for d in data_source]):
        raise Exception("Error: One or more of `data_source` are not available.")

    if not all([i in tools._gadm_levels() for i in admin_level]):
        raise Exception("Error: One or more of `admin_level` are not available.")

    if gadm_version not in tools._gadm_versions():
        raise Exception("Error: GADM version not available.")

    if not os.path.exists(dir_from):
        raise Exception("Error: Data source directory `dir_from` not found.")

    if not os.path.exists(dir_to):
        raise Exception("Error: Data output directory `dir_to` not found.")

    # currently offering only data source and admin level combinations
    for d in data_source:
        for i in admin_level:
            in_files = tools._find_clim_files(dir_from, d, i, gadm_version)
            out_file = os.path.join(dir_to, tools._make_output_names(d, i))
            tools._files_to_parquet(in_files, out_file)
