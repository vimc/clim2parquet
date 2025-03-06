"""Tools to find and convert climate data files from CSV to parquet."""

import importlib.resources
import logging
import re
import warnings
from pathlib import Path

import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore


def _get_data_info() -> pd.DataFrame:
    """
    Get climate data source information as a Pandas DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the data source names, regex patterns, and output
        names. Data version information will be included in future.
    """
    with (
        importlib.resources.files("clim2parquet.data")
        .joinpath("data_sources.csv")
        .open("r", encoding="utf-8")
    ) as f:
        return pd.read_csv(f, dtype=str)


def _get_level_pattern(admin_level: int, gadm_version: str = "v410") -> str:
    """
    Get file naming pattern for a GADM admin level.

    Parameters
    ----------
    admin_level : int
        GADM admin level as an integer. Must be in the range 0 -- 3.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0. No other
        versions are currently supported.

    Returns
    -------
    A string for a regex pattern to search for files at a specific admin level.
    """
    rep_level = "\\d+_" * (admin_level + ((admin_level > 0) * 1))
    level_pattern = f"{gadm_version}_{rep_level}"
    return level_pattern


def _get_files_size(files: list[Path]) -> float:
    """
    Get the total size of a list of files in megabytes.

    Parameters
    ----------
    files : list[Path]
        List of file paths as `pathlib.Path` objects.

    Returns
    -------
    A `float` representing the total size of the files in bytes.
    """
    size = 0
    for f in files:
        size += f.stat().st_size
    return size


def _find_clim_files(
    dir_from: Path,
    data_source: str,
    admin_level: int,
    gadm_version: str = "v410",
) -> list[Path]:
    """
    Find climate data files for a given GADM admin level and data source.

    Parameters
    ----------
    dir_from : pathlib.Path
        Path to the directory containing the data files, as a `pathlib.Path`.
    data_source : str
        The data source name. See available data sources in `get_data_names()`.
    admin_level : int
        GADM admin level as an integer. Must be in the range 0 -- 3.
    gadm_version : str
        GADM version as a string. Default is "v410" for v4.1.0. No other
        versions are currently supported.

    Returns
    -------
    A list of file paths corresponding to data for the given data source and
        admin level. Prints the number of files found and their total size
        to the console as a side effect.
    """
    files = list(dir_from.iterdir())  # NOTE: returns full filepath

    # get regex to search directory for data files
    data_info = _get_data_info()
    data_pattern = data_info.loc[
        data_info["data_source"] == data_source, "data_regex"
    ].item()

    pattern = f"{_get_level_pattern(admin_level, gadm_version)}{data_pattern}"
    path_files = [f for f in files if re.search(pattern, str(f))]

    if len(path_files) < 1:
        warnings.warn(
            f"Found no {data_source} files for admin level {admin_level} under \
                `{dir_from}`!",
            stacklevel=2,
        )
    else:
        files_size = _get_files_size(files)
        logging.info(
            f"Found {len(files)} files with a total size of \
                {files_size:.2f} B."
        )

    return path_files


def _gadm_levels() -> list[int]:
    """
    Supported GADM admin levels.

    Returns
    -------
    A list of integers representing the supported GADM admin levels.
    """
    return [0, 1, 2, 3]


def _gadm_versions() -> list[str]:
    """
    Supported GADM version strings.

    Returns
    -------
    A list of strings representing the supported GADM versions. Currently only
        v4.1.0 is supported.

    """
    return ["v410"]


def _make_output_names(data_source: str, admin_level: int) -> str:
    """
    Make output file names for a given data source and GADM admin level.

    Parameters
    ----------
    data_source : str
        The data source name. See available data sources in `get_data_names()`.
    admin_level : int
        GADM admin level as an integer. Must be in the range 0 -- 3.

    Returns
    -------
    A string for the output file name. Does not contain the output directory.
    """
    data_info = _get_data_info()
    data_output_name = data_info.loc[
        data_info["data_source"] == data_source, "data_output_name"
    ].item()

    return f"{data_output_name}_admin_{admin_level}.parquet"


def _files_to_parquet(files: list[Path], to: str) -> None:
    """
    Convert country data from CSV to Parquet file.

    Parameters
    ----------
    files : list[Path]
        List of data files, assumed to be CSVs, as `pathlib.Path`.
    to : str
        Path and filename to output the data. Currently `str` as `pyarrow`
        does not support `pathlib.Path`.

    Returns
    -------
    None. Called only for the side effect of converting CSV data files to
    Parquet.
    """
    data_list = []
    # NOTE: function will error if there are no files
    for file in files:
        df = pd.read_csv(file, sep=",", header=0)
        data_list.append(df)

    df = pd.concat(data_list)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, to)
