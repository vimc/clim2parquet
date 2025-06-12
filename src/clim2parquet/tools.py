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


def _data_source_info() -> pd.DataFrame:
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


def _data_admin_unit_ids() -> pd.DataFrame:
    """
    Get package data on admin unit identifiers as a Pandas DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame with the admin unit identifiers.

        The DataFrame has 9 columns corresponding to `GID_X` and `NAME_X` where
        `X` can be one of `[0, 1, 2, 3]`. These columns hold values taken from
        GADM spatial data.

        An additional integer column `admin_unit_id` gives the
        admin unit identifier.

        See the package data preparation script `data-raw/admin_unit_ids.py` for
        how this data is prepared, and see the package documentation on how this
        data relates to GADM admin unit identifiers.
    """
    with (
        importlib.resources.files("clim2parquet.data")
        .joinpath("admin_units.parquet")
        .open("rb")
    ) as f:
        return pd.read_parquet(f)


def _data_country_codes() -> list[str]:
    """
    Get ISO 3 character country codes from package data.

    Returns
    -------
    list[str]
        A list of ISO 3 character country codes. This list is formed by taking
        the `GID_0` (country level identifier) from admin unit identifier data
        prepared in `data-raw/admin_unit_ids.py`.
    """
    with (
        importlib.resources.files("clim2parquet.data")
        .joinpath("country_codes.csv")
        .open("r", encoding="utf8")
    ) as f:
        cc_df = pd.read_csv(f, dtype=str)
        return cc_df["GID_0"].tolist()


def _get_level_pattern(admin_level: int, gadm_version: str) -> str:
    """
    Get file naming pattern for a GADM admin level.

    Parameters
    ----------
    admin_level : int
        GADM admin level as an integer. Must be in the range 0 -- 3.
    gadm_version : str
        GADM version as a string. No default as this is passed from higher level
        functions.

    Returns
    -------
    str
        A regex pattern to search for files at a specific admin level.
    """
    # NOTE: GADM level 0 has no level or GID code annotation
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
    dir_from: Path, data_source: str, admin_level: int, gadm_version: str
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
        GADM version as a string. No default for this internal function as this
        is passed from higher level functions.

    Returns
    -------
    list[Path]
        File paths corresponding to data for the given data source and
        admin level. Prints the number of files found and their total size
        to the console as a side effect.
    """
    files = list(dir_from.iterdir())  # NOTE: returns full filepath

    # get regex to search directory for data files
    data_info = _data_source_info()
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
            f"Found {len(path_files)} '{data_source}' files at "
            f"admin level {admin_level}; "
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
    data_info = _data_source_info()
    data_output_name = data_info.loc[
        data_info["data_source"] == data_source, "data_output_name"
    ].item()

    return f"{data_output_name}_admin_{admin_level}.parquet"


def _get_country_code(filename: str, gadm_version: str) -> str:
    """
    Get the ISO 3 character country code from a filename.

    Parameters
    ----------
    filename : str
        The filename of the data file.
    gadm_version : str
        GADM version as a string. No default as this is passed from higher level
        functions.

    Returns
    -------
    str
        The ISO 3 character country code extracted from the filename.
    """
    pattern = rf"([A-Z]{{3}})_{gadm_version}"
    match = re.search(pattern, filename)
    if match:
        match_0 = match.group(0)
        match_0 = match_0.strip(f"_{gadm_version}")
        if match_0 not in _data_country_codes():
            err_cc_not_recog = f"Country code of {filename} not recognised."
            raise Exception(err_cc_not_recog)
        else:
            return match_0  # type: ignore
    else:
        err_cc_not_found = "Country code not found in filename."
        raise Exception(err_cc_not_found)


def _get_admin_data(
    filename: str, admin_level: int, gadm_version: str
) -> list[str | None]:
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
    list[str|None]
        A list of 4 elements, where the first element is always a string, but
        remaining elements may be `None`.
        Each element represents the `GID_X` identifier constructed from the
        filename, where `X` is in `[0, 1, 2, 3]` for the allowed GADM levels.

        Level 0 is represented as `ABC` for the ISO 3 character country code,
        while levels 1 - 3 are represented as `ABC.1.2.3_Z` respectively.
        The appended `Z` is the GID_code, which is also inferred from the file
        name.

        **Note that** the GID code for all admin levels is taken from the file
        name.
    """
    level_pattern = _get_level_pattern(admin_level, gadm_version)
    level_match = re.search(level_pattern, filename)

    country_code = _get_country_code(filename, gadm_version)

    match_0 = level_match.group(0)  # type: ignore
    match_0 = match_0.strip(gadm_version)
    match_0 = match_0.strip("_")

    # return a list of admin unit identifiers that match GADM and the index file
    if match_0:
        admin_data = re.findall(r"\d+", match_0)
        gid_code = admin_data[-1]  # removed here and re-added later
        admin_data = admin_data[:-1]

        admin_data = [".".join(admin_data[: i + 1]) for i in range(admin_level)]
        admin_data = [f"{country_code}.{i}_{gid_code}" for i in admin_data]
        admin_data = [country_code, *admin_data]
    else:
        admin_data = [country_code]

    # pad with None if len < 4
    needed_len = len(_gadm_levels())
    if len(admin_data) < needed_len:
        pad_list = [None] * (needed_len - len(admin_data))
        admin_data = [*admin_data, *pad_list]

    return admin_data


def _add_admin_unit_id(
    data: pd.DataFrame,
    admin_data: list[str | None],
    admin_unit_ids: pd.DataFrame,
) -> None:
    """
    Add admin unit identifier to a dataframe of climate data.

    Parameters
    ----------
    data : pd.DataFrame
        A Pandas DataFrame containing climate data.
    admin_data : list[str|None]
        A list of strings and `None` with GADM admin-unit data,
        typically extracted from the filename using `_get_admin_data()`.
    admin_unit_ids : pd.DataFrame
        A Pandas DataFrame containing the admin unit identifiers, returned from
        `_data_admin_unit_ids()`.

    Returns
    -------
    None
        Called only for the side effect of modifying the input `DataFrame`.
        Note that this function modifies the input `DataFrame` in place.
    """
    # prepare boolean mask for admin unit ids dataframe
    colnames = [f"GID_{i}" for i in _gadm_levels()]

    condition = pd.DataFrame(
        {
            col: (admin_unit_ids[col] == val)
            | (pd.isna(admin_unit_ids[col]) & pd.isna(val))
            for col, val in zip(colnames, admin_data)
        }
    )

    admin_unit = admin_unit_ids[condition.all(axis=1)]
    admin_unit_data_id = admin_unit["admin_unit_id"].values[0]

    # NOTE: modification in place
    data["admin_unit_id"] = admin_unit_data_id


def _files_to_parquet(
    files: list[Path],
    to: str,
    admin_level: int,
    admin_unit_ids: pd.DataFrame,
    gadm_version: str,
) -> None:
    """
    Convert country data from CSV to Parquet file.

    Parameters
    ----------
    files : list[Path]
        List of data files, assumed to be CSVs, as `pathlib.Path`.
    to : str
        Path and filename to output the data. Currently `str` as `pyarrow`
        does not support `pathlib.Path`.
    admin_unit_ids : pd.DataFrame
        A Pandas DataFrame containing the admin unit UIDs. This is used to
        map the admin data to the correct UIDs.

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
        _add_admin_unit_id(
            df,
            _get_admin_data(str(file), admin_level, gadm_version),
            admin_unit_ids,
        )
        data_list.append(df)

    df = pd.concat(data_list)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, to, compression = "zstd", compression_level = 9)
