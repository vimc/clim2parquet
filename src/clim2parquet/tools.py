import importlib.resources
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re


def _get_data_info() -> pd.DataFrame:
    """Loads the default dataset from the package data directory into a Pandas DataFrame."""
    with (
        importlib.resources.files("clim2parquet.data")
        .joinpath("data_sources.csv")
        .open("r", encoding="utf-8") as f
    ):
        return pd.read_csv(f, dtype=str)


def _get_level_pattern(admin_level: int, gadm_version: str = "v410"):
    """
    Get file naming pattern for a GADM admin level.
    """
    return gadm_version + "_" + "\\d+_" * (admin_level + ((admin_level > 0) * 1))


def _get_files_size(files: list):
    """
    Get the total size of a list of files in megabytes.
    """
    size = 0
    for f in files:
        size += os.path.getsize(f)
    return size / 1e6


def _find_clim_files(
    dir: str, data_source: str, admin_level: int, gadm_version: str = "v410"
):
    """
    Find climate data files for a given GADM admin level and data source.
    Prints the number of files found and their total size to the console.
    """
    files = os.listdir(dir)

    # get regex to search directory for data files
    data_info = _get_data_info()
    data_pattern = data_info.loc[
        data_info["data_source"] == data_source, "data_regex"
    ].item()

    pattern = _get_level_pattern(admin_level, gadm_version) + data_pattern
    files = [os.path.join(dir, f) for f in files if re.search(pattern, f)]

    if len(files) == 0:
        print(f"Warning: Found no {data_source} files under `{dir}`!")
    else:
        files_size = _get_files_size(files)
        print(f"Found {len(files)} files with a total size of {files_size:.2f} MB.")

    return files


def _gadm_levels():
    """
    Supported GADM admin levels.
    """
    return [0, 1, 2, 3]


def _gadm_versions():
    """
    Supported GADM version strings.
    """
    return ["v410"]


def _get_data_names():
    """
    Get data source names.
    """
    return _get_data_info()["data_source"].tolist()


def _make_output_names(data_source: str, admin_level: int):
    """
    Make output file names for a given data source and GADM admin level.
    """
    data_info = _get_data_info()
    data_output_name = data_info.loc[
        data_info["data_source"] == data_source, "data_output_name"
    ].item()

    return data_output_name + "_admin_" + str(admin_level) + ".parquet"


def _files_to_parquet(files: list, to: str):
    """
    Convert country data from CSV to Parquet file.

    Parameters
    ----------
    files : list
        List of data files, assumed to be CSVs.
    to : str
        Path and filename to output the data.
    """
    data_list = []
    for file in files:
        df = pd.read_csv(file, sep=",", header=0)
        data_list.append(df)

    df = pd.concat(data_list)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, to)
