# SPDX-FileCopyrightText: 2025-present Imperial College of Science and Technology
#
# SPDX-License-Identifier: MIT

from . import tools

import os


def chirps_to_parquet(dir: str, admin_level: int, to: str, gadm_version: str = "v410"):
    """
    Convert country CHIRPS data to a Parquet file.

    Parameters
    ----------
    dir : str
        Path to the data directory.
    admin_level : int
        GADM admin level as a single integer.
    to : str
        Path and filename to output the data.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """
    data_source = "CHIRPS"
    files = tools._find_clim_files(dir, data_source, admin_level, gadm_version)

    if len(files) == 0:
        print(f"Warning: Found no CHIRPS files under `{dir}`!")
        return

    tools._files_to_parquet(files, to)


def era5mean_to_parquet(
    dir: str, admin_level: int, to: str, gadm_version: str = "v410"
):
    """
    Convert country CHIRPS data to a Parquet file.

    Parameters
    ----------
    dir : str
        Path to the data directory.
    admin_level : int
        GADM admin level as a single integer.
    to : str
        Path and filename to output the data.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """
    data_source = "ERA5_Land_2m_temperature"
    files = tools._find_clim_files(dir, data_source, admin_level, gadm_version)
    exclude_pattern = "day"
    files = [f for f in files if exclude_pattern not in os.path.basename(f)]

    if len(files) == 0:
        print(f"Warning: Found no ERA5 mean temp files under `{dir}`!")
        return

    tools._files_to_parquet(files, to)


def era5min_to_parquet(dir: str, admin_level: int, to: str, gadm_version: str = "v410"):
    """
    Convert country ERA5 land 2m daily minimum air temperature data to a Parquet
    file.

    Parameters
    ----------
    dir : str
        Path to the data directory.
    admin_level : int
        GADM admin level as a single integer.
    to : str
        Path and filename to output the data.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """
    data_source = "ERA5_Land_2m_temperature_daymin"
    files = tools._find_clim_files(dir, data_source, admin_level, gadm_version)

    if len(files) == 0:
        print(f"Warning: Found no ERA5 min. temp. files under `{dir}`!")
        return

    tools._files_to_parquet(files, to)


def era5max_to_parquet(dir: str, admin_level: int, to: str, gadm_version: str = "v410"):
    """
    Convert country ERA5 land 2m daily maximum air temperature data to a Parquet
    file.

    Parameters
    ----------
    dir : str
        Path to the data directory.
    admin_level : int
        GADM admin level as a single integer.
    to : str
        Path and filename to output the data.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """
    data_source = "ERA5_Land_2m_temperature_daymax"
    files = tools._find_clim_files(dir, data_source, admin_level, gadm_version)

    if len(files) == 0:
        print(f"Warning: Found no ERA5 max. temp. files under `{dir}`!")
        return

    tools._files_to_parquet(files, to)


def era5rh_to_parquet(dir: str, admin_level: int, to: str, gadm_version: str = "v410"):
    """
    Convert country ERA5 land 2m daily relative humidity data to a Parquet file.

    Parameters
    ----------
    dir : str
        Path to the data directory.
    admin_level : int
        GADM admin level as a single integer.
    to : str
        Path and filename to output the data.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """
    data_source = "ERA5_Land_relative_humidity"
    files = tools._find_clim_files(dir, data_source, admin_level, gadm_version)

    if len(files) == 0:
        print(f"Warning: Found no ERA5 RH files under `{dir}`!")
        return

    tools._files_to_parquet(files, to)


def era5sh_to_parquet(dir: str, admin_level: int, to: str, gadm_version: str = "v410"):
    """
    Convert country ERA5 land 2m daily specific humidity data to a Parquet file.

    Parameters
    ----------
    dir : str
        Path to the data directory.
    admin_level : int
        GADM admin level as a single integer.
    to : str
        Path and filename to output the data.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """
    data_source = "ERA5_Land_specific_humidity"
    files = tools._find_clim_files(dir, data_source, admin_level, gadm_version)

    if len(files) == 0:
        print(f"Warning: Found no ERA5 SH files under `{dir}`!")
        return

    tools._files_to_parquet(files, to)


def persiann_to_parquet(
    dir: str, admin_level: int, to: str, gadm_version: str = "v410"
):
    """
    Convert country ERA5 land 2m daily specific humidity data to a Parquet file.

    Parameters
    ----------
    dir : str
        Path to the data directory.
    admin_level : int
        GADM admin level as a single integer.
    to : str
        Path and filename to output the data.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """
    data_source = "PERSIANN"
    files = tools._find_clim_files(dir, data_source, admin_level, gadm_version)

    if len(files) == 0:
        print(f"Warning: Found no PERSIANN files under `{dir}`!")
        return

    tools._files_to_parquet(files, to)


def clim_to_parquet(
    dir: str, admin_level: int, dir_to: str, gadm_version: str = "v410"
):
    """
    Convert country climate data to a Parquet file.

    Parameters
    ----------
    dir : str
        Path to the data directory.
    dir_to: str
        Path to the output directory.
    admin_level : int
        GADM admin level as a single integer.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0.
    """

    # file names
    chirps_file = "chirps_admin_" + str(admin_level) + ".parquet"
    era5mean_file = "era5mean_admin_" + str(admin_level) + ".parquet"

    chirps_to_parquet(dir, admin_level, chirps_file, gadm_version)
    era5mean_to_parquet(dir, admin_level, era5mean_file, gadm_version)
