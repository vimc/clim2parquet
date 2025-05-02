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
    data_info = _get_data_info()
    data_output_name = data_info.loc[
        data_info["data_source"] == data_source, "data_output_name"
    ].item()

    return f"{data_output_name}_admin_{admin_level}.parquet"


def _pad_admin_levels(
    admin_data: list[int], admin_level: int, pad_len: int = 3
) -> list[int]:
    """
    Pad admin data list to a fixed length.

    Parameters
    ----------
    admin_data : list[int]
        A list of integers representing the GADM admin levels. Note that this
        function expects that the GID code at the end has been removed.
    admin_level: int
        The inferred admin level.
    pad_len : int
        The padded length of the list. Default is 3.
        The list is padded with zeros to the right if the inferred admin level
        is 0, and with a single zero to the left and zeros to the right if the
        inferred admin level is 1 -- 3.
    """
    admin_0_identifier = 0

    # admin 0 and admin 1 have similar signatures
    zero_counter = 1 if admin_level > 0 else 0
    pad_len = pad_len + 1 if admin_level == 0 else pad_len

    admin_data = (
        [admin_0_identifier] * zero_counter
        + admin_data
        + [0] * (pad_len - len(admin_data))
    )

    return admin_data


def _get_admin_data(
    filename: str, admin_level: int, gadm_version: str
) -> list[int]:
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
    list[int]
        A list of 5 integers. For GADM level 0 (country level), the list
        `[0, 0, 0, 0 1]` is returned indicating country level data and a GADM
        identification code (GID code) version of 1.0. No other admin units
        have a numeric identifier of 0.

        For all levels > 0, a list where the last element
        is the GID code version. Of the preceding N elements, the i-th element
        is the numeric identifier of the i-th admin level. The first element is
        always 0, indicating the country level.

        For example, the file `'*_v410_1_2_3_4_clim.csv'` would return
        [0, 1, 2, 3, 4], where 1, 2, and 3 are the numeric identifiers of
        the 1st, 2nd, and 3rd admin levels, respectively, and 4 is the GID code
        version. 0 is the country level identifier.
    """
    pattern = _get_level_pattern(admin_level, gadm_version)
    match = re.search(pattern, filename)

    match_0 = match.group(0)  # type: ignore
    match_0 = match_0.strip(gadm_version)
    match_0 = match_0.strip("_")

    inferred_admin_level = 0
    if match_0:
        admin_data = re.findall(r"\d+", match_0)
        admin_data = [int(i) for i in admin_data]
        gid_code = admin_data[-1]
        admin_data = admin_data[:-1]  # strip GID code
        inferred_admin_level = len(admin_data)
    else:
        # admin level is 0 (country)
        # GID code version assumed to be 1
        admin_data = [0]
        gid_code = 1

    # process into a 5-element list, filling zeros for lower admin units
    # and adding GID code
    admin_data = _pad_admin_levels(admin_data, inferred_admin_level)
    admin_data.append(gid_code)

    return admin_data


def _add_admin_data(
    data: pd.DataFrame, admin_data: list[int], admin_unit_uids: pd.DataFrame
) -> None:
    """
    Add GADM admin-level and admin-unit data to a dataframe of climate data.

    Parameters
    ----------
    data : pd.DataFrame
        A Pandas DataFrame containing climate data.
    admin_data : list[int]
        A list of integers with GADM admin-unit data and GID code version,
        typically extracted from the filename using `_get_admin_data()`.
    admin_unit_uids : pd.DataFrame
        A Pandas DataFrame containing the admin unit UIDs. This is used to
        map the admin data to the correct UIDs.

    Returns
    -------
    None
        Called only for the side effect of modifying the input `DataFrame`.
        Note that this function modifies the input `DataFrame` in place.
    """
    # filter admin_data dataframe for values in admin_data
    admin_unit_uids = admin_unit_uids[
        (admin_unit_uids["admin_unit_0"] == admin_data[0])
        & (admin_unit_uids["admin_unit_1"] == admin_data[1])
        & (admin_unit_uids["admin_unit_2"] == admin_data[2])
        & (admin_unit_uids["admin_unit_3"] == admin_data[3])
        & (admin_unit_uids["gid_code_version"] == admin_data[4])
    ]

    admin_unit_uid = admin_unit_uids["uid"].values[0]
    # NOTE: modification in place
    data["admin_unit_uid"] = admin_unit_uid


def _files_to_parquet(
    files: list[Path],
    to: str,
    admin_level: int,
    admin_unit_uids: pd.DataFrame,
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
    admin_unit_uids : pd.DataFrame
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
        _add_admin_data(
            df,
            _get_admin_data(str(file), admin_level, gadm_version),
            admin_unit_uids,
        )
        data_list.append(df)

    df = pd.concat(data_list)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, to)
