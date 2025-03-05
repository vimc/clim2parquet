import clim2parquet
import os
import pytest
import tempfile
import warnings

# Test all data-specific functions and main wrapper function clim_to_parquet()
# for admin level 0 (country level)


# Test conversion from specific data source
def test_clim_to_parquet():
    """Test that climate data parquet files have been created."""

    admin_level = 0
    with tempfile.TemporaryDirectory() as temp_dir:
        # admin level defaults to 0
        clim2parquet.clim_to_parquet("CHIRPS", "tests/test-data/country_A/", temp_dir)

        file_name = clim2parquet.tools._make_output_names("CHIRPS", admin_level)

        f = os.path.join(temp_dir, file_name)
        assert os.path.exists(f)

    with tempfile.TemporaryDirectory() as temp_dir:
        # admin level defaults to 0
        clim2parquet.clim_to_parquet(
            "ERA5_mean", "tests/test-data/country_A/", temp_dir
        )

        file_name = clim2parquet.tools._make_output_names("ERA5_mean", admin_level)

        f = os.path.join(temp_dir, file_name)
        assert os.path.exists(f)


def test_multiclim_to_parquet():
    """Test that multiple climate data parquet files have been created."""

    admin_level = 0
    with tempfile.TemporaryDirectory() as temp_dir:
        # admin level defaults to 0
        data_sources = ["CHIRPS", "ERA5_mean"]
        clim2parquet.clim_to_parquet(
            data_sources, "tests/test-data/country_A/", temp_dir
        )

        file_names = [
            clim2parquet.tools._make_output_names(d, admin_level) for d in data_sources
        ]

        for f in file_names:
            f = os.path.join(temp_dir, f)
            assert os.path.exists(f)


# Test that CHIRPS to parquet works for admin 1
# Data for further levels not included in test data
def test_chirps_to_parquet_admin_1():
    """Test that climate data parquet files have been created."""

    admin_level = 1
    with tempfile.TemporaryDirectory() as temp_dir:
        # admin level defaults to 0
        clim2parquet.clim_to_parquet(
            "CHIRPS", "tests/test-data/country_A/", temp_dir, admin_level
        )

        file_name = clim2parquet.tools._make_output_names("CHIRPS", admin_level)

        f = os.path.join(temp_dir, file_name)
        assert os.path.exists(f)


# Tests for errors
def test_clim_to_parquet_errors():
    with pytest.raises(ValueError, match=r"One or more of `data_source`"):
        clim2parquet.clim_to_parquet("dummy_option", ".", ".")

    excess_admin_level = 99
    with pytest.raises(ValueError, match=r"One or more of `admin_level`"):
        clim2parquet.clim_to_parquet("CHIRPS", ".", ".", admin_level=excess_admin_level)

    bad_gadm_version = "v4"
    with pytest.raises(ValueError, match=r"GADM version not available"):
        clim2parquet.clim_to_parquet("CHIRPS", ".", ".", 0, bad_gadm_version)

    # test for warning when files are absent
    data_sources = "PERSIANN"  # PERSIANN data not included at GADM level 1
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        clim2parquet.clim_to_parquet(data_sources, "tests/test-data/country_A", ".", 1)
        assert len(w) == 1
        assert issubclass(w[-1].category, Warning)
        assert "No files found" in str(w[-1].message)
