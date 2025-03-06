"""Test conversion from CSV to Parquet using top-level function."""

import warnings
from pathlib import Path

import pytest

import clim2parquet


# Test all data-specific functions and main wrapper function clim_to_parquet()
# for admin level 0 (country level)
# Test conversion from specific data source
def test_clim_to_parquet(tmp_path: Path) -> None:
    """Test that climate data parquet files have been created."""
    admin_level = 0
    # admin level defaults to 0

    path_from = Path("tests/test-data/country_A/")
    clim2parquet.clim_to_parquet("CHIRPS", path_from, tmp_path)
    file_name = clim2parquet.tools._make_output_names("CHIRPS", admin_level)
    assert (tmp_path / file_name).exists()

    clim2parquet.clim_to_parquet(
        "ERA5_mean", "tests/test-data/country_A/", tmp_path
    )
    file_name = clim2parquet.tools._make_output_names("ERA5_mean", admin_level)
    assert (tmp_path / file_name).exists()


def test_multiclim_to_parquet(tmp_path: Path) -> None:
    """Test that multiple climate data parquet files have been created."""
    admin_level = 0
    # admin level defaults to 0
    data_sources = ["CHIRPS", "ERA5_mean"]
    clim2parquet.clim_to_parquet(
        data_sources, "tests/test-data/country_A/", tmp_path
    )

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
    clim2parquet.clim_to_parquet(
        "CHIRPS", "tests/test-data/country_A/", tmp_path, admin_level
    )
    file_name = clim2parquet.tools._make_output_names("CHIRPS", admin_level)

    assert (tmp_path / file_name).exists()


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
        clim2parquet.clim_to_parquet(
            "CHIRPS", "tests/test-data/country_A/", bad_dir_to
        )

    # test for warning when files are absent
    data_sources = "PERSIANN"  # PERSIANN data not included at GADM level 1
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        clim2parquet.clim_to_parquet(
            data_sources, "tests/test-data/country_A", ".", 1
        )
        assert len(w) == 1
        assert issubclass(w[-1].category, Warning)
        assert f"Found no {data_sources} files" in str(w[-1].message)
