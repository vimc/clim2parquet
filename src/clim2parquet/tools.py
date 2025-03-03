import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re


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
    pattern = _get_level_pattern(admin_level, gadm_version) + data_source
    files = [os.path.join(dir, f) for f in files if re.search(pattern, f)]

    # Print file count and total size
    files_size = _get_files_size(files)
    print(f"Found {len(files)} files with a total size of {files_size:.2f} MB.")

    return files


def _files_to_parquet(files: list, to: str):
    """
    Convert country data to a Parquet file.

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
