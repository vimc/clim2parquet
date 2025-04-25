"""Test conversion from CSV to Parquet using top-level function."""

import warnings
from pathlib import Path

import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
import pytest

import clim2parquet

path_from = Path("tests/test-data/ZZZ/")


# Test all data-specific functions and main wrapper function clim_to_parquet()
# for admin level 0 (country level)
# Test conversion from specific data source
def test_clim_to_parquet(tmp_path: Path) -> None:
    """Test that climate data parquet files have been created."""
    admin_level = 0

    clim2parquet.clim_to_parquet("CHIRPS", path_from, tmp_path)
    file_name = clim2parquet.tools._make_output_names("CHIRPS", admin_level)
    assert (tmp_path / file_name).exists()

    clim2parquet.clim_to_parquet("ERA5_mean", path_from, tmp_path)
    file_name = clim2parquet.tools._make_output_names("ERA5_mean", admin_level)
    assert (tmp_path / file_name).exists()


def test_multiclim_to_parquet(tmp_path: Path) -> None:
    """Test that all climate data sources can be converted."""
    admin_level = 0

    data_sources = clim2parquet.get_data_names()
    clim2parquet.clim_to_parquet(data_sources, path_from, tmp_path)

    file_names = [
        clim2parquet.tools._make_output_names(d, admin_level)
        for d in data_sources
    ]

    for f in file_names:
        assert (tmp_path / f).exists()


# Test that CHIRPS to parquet works for admin 1
# Data for further levels not included in test data
def test_chirps_to_parquet_admin_1(tmp_path: Path) -> None:
    """Test that climate data parquet files have been created."""
    admin_level = 1

    clim2parquet.clim_to_parquet("CHIRPS", path_from, tmp_path, admin_level)
    file_name = clim2parquet.tools._make_output_names("CHIRPS", admin_level)

    assert (tmp_path / file_name).exists()


# Test output conforms to expectations for country level data
def test_output_format_lvl_0(tmp_path: Path) -> None:
    """Test that Parquet output has required format for country level data."""
    admin_level = 0

    file_name = clim2parquet.tools._make_output_names("ERA5_RH", admin_level)
    clim2parquet.clim_to_parquet("ERA5_RH", path_from, tmp_path, admin_level)
    data = pq.read_table(tmp_path / file_name)

    admin_info_cols = "admin_unit_uid"

    assert isinstance(data, pa.Table)
    assert admin_info_cols in data.column_names
    # assert len(set(data[admin_info_cols])) == 1
    # assert set(data[admin_info_cols]) == {0}


# Test output conforms to expectations for subnational data
def test_output_format_lvl_1(tmp_path: Path) -> None:
    """Test that Parquet output has required format for subnational data."""
    admin_level = 1

    file_name = clim2parquet.tools._make_output_names("CHIRPS", admin_level)
    clim2parquet.clim_to_parquet("CHIRPS", path_from, tmp_path, admin_level)
    data = pq.read_table(tmp_path / file_name)

    admin_info_cols = "admin_unit_uid"

    assert isinstance(data, pa.Table)
    assert admin_info_cols in data.column_names
    # assert len(set(data[admin_info_cols])) > 1


# Test output conforms to expectations for subnational data
def test_output_format_lvl_n(tmp_path: Path) -> None:
    """Test that Parquet output has required format for subnational data."""
    admin_level = 3

    file_name = clim2parquet.tools._make_output_names("ERA5_mean", admin_level)
    clim2parquet.clim_to_parquet("ERA5_mean", path_from, tmp_path, admin_level)
    data = pq.read_table(tmp_path / file_name)

    admin_info_cols = "admin_unit_uid"

    assert isinstance(data, pa.Table)
    assert admin_info_cols in data.column_names
    # assert len(set(data[admin_info_cols])) > 1


# Tests for errors
def test_clim_to_parquet_errors() -> None:
    """Check for errors in clim_to_parquet()."""
    with pytest.raises(ValueError, match=r"One or more of `data_source`"):
        clim2parquet.clim_to_parquet("dummy_option", ".", ".")

    excess_admin_level = 99
    with pytest.raises(ValueError, match=r"One or more of `admin_level`"):
        clim2parquet.clim_to_parquet(
            "CHIRPS", ".", ".", admin_level=excess_admin_level
        )

    bad_gadm_version = "v4"
    with pytest.raises(ValueError, match=r"GADM version not available"):
        clim2parquet.clim_to_parquet("CHIRPS", ".", ".", 0, bad_gadm_version)

    bad_dir_from = "dummy_from"
    with pytest.raises(Exception, match=r"Data source directory"):
        clim2parquet.clim_to_parquet("CHIRPS", bad_dir_from, ".")

    bad_dir_to = Path("./dummy_to")
    with pytest.raises(Exception, match=r"Data output directory"):
        clim2parquet.clim_to_parquet("CHIRPS", path_from, bad_dir_to)

    # test for warning when files are absent
    data_sources = "PERSIANN"  # PERSIANN data not included at GADM level 1
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        clim2parquet.clim_to_parquet(data_sources, path_from, ".", 1)
        assert len(w) == 1
        assert issubclass(w[-1].category, Warning)
        assert f"Found no {data_sources} files" in str(w[-1].message)
