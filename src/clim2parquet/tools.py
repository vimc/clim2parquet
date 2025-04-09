"""Tools to find and convert climate data files from CSV to parquet."""

import importlib.resources
import logging
import re
import warnings
from pathlib import Path

import pandas as pd
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(console_handler)


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
    str
        A regex pattern to search for files at a specific admin level.
    """
    rep_count = admin_level + ((admin_level > 0) * 1)
    level_pattern = rf"{gadm_version}_(\d+_){{{rep_count}}}"
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
    float
        The total size of the files in bytes.
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
    list[Path]
        File paths corresponding to data for the given data source and
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
            f"Found no {data_source} files for admin level {admin_level} "
            f"under '{dir_from}'!",
            stacklevel=2,
        )
    else:
        files_size = _get_files_size(path_files)
        logger.info(
            f"Found {len(path_files)} '{data_source}' files; "
            f"total size = {files_size:.2f} B."
        )

    return path_files


def _gadm_levels() -> list[int]:
    """
    Supported GADM admin levels.

    Returns
    -------
    list[int]
        A list of integers representing the supported GADM admin levels.
    """
    return [0, 1, 2, 3]


def _gadm_versions() -> list[str]:
    """
    Supported GADM version strings.

    Returns
    -------
    list[str]
        A list of strings representing the supported GADM versions. Currently
        only v4.1.0 is supported.

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
    str
        A string for the output file name. Does not contain the output
        directory.
    """
    data_info = _get_data_info()
    data_output_name = data_info.loc[
        data_info["data_source"] == data_source, "data_output_name"
    ].item()

    return f"{data_output_name}_admin_{admin_level}.parquet"


def _get_admin_data(
    filename: str, admin_level: int, gadm_version: str
) -> list[str]:
    """
    Get GADM admin-level and admin-unit information from a filename.

    Parameters
    ----------
    filename : str
        The filename of the data file.
    admin_level : int
        GADM admin level as an integer.
    gadm_version : str
        GADM version as a string. No default as this is passed from higher level
        functions.

    Returns
    -------
    list[str]
        A list of strings. For GADM level 0 (country level), the list
        `['0', '1']` is returned indicating country level data and a GADM
        identification code (GID code) version of 1.0. No other admin units
        have a numeric identifier of 0.

        For all levels > 0, a list with N + 1 elements, where the last element
        is the GID code version. Of the preceding N elements, the i-th element
        is the numeric identifier of the i-th admin level.

        For example, the file `'*_v410_1_2_3_4_clim.csv'` would return
        ['1', '2', '3', '4'], where 1, 2, and 3 are the numeric identifiers of
        the 1st, 2nd, and 3rd admin levels, respectively, and 4 is the GID code
        version.
    """
    pattern = _get_level_pattern(admin_level, gadm_version)
    match = re.search(pattern, filename).group(0)
    match = match.strip(gadm_version)
    match = match.strip("_")

    if match:
        matches = re.findall(r"\d+", match)
        return matches
    else:
        # admin level is 0 (country)
        # GID code version assumed to be 1
        return ["0", "1"]


def _add_admin_data(data: pd.DataFrame, admin_data: list[str]) -> None:
    """
    Add GADM admin-level and admin-unit data to a dataframe of climate data.

    Parameters
    ----------
    data : pd.DataFrame
        A Pandas DataFrame containing climate data.
    admin_data : list[str]
        A list of strings with GADM admin-unit data and GID code version,
        typically extracted from the filename using `_get_admin_data()`.

    Returns
    -------
    None
        Called only for the side effect of modifying the input `DataFrame`.
        Note that this function modifies the input `DataFrame` in place.
    """
    gid_code_version = admin_data[-1]

    # handle special case of admin level 0 (country level)
    level_bump = 0 if (admin_data[0] == "0") else 1

    # NOTE: modification in place
    for i, value in enumerate(admin_data[:-1]):
        data[f"admin_unit_{i + level_bump}"] = value
    data["gid_code_version"] = gid_code_version


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
    None
        Called only for the side effect of converting CSV data files to
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
