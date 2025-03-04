import clim2parquet
import os
import tempfile

# Test all data-specific functions and main wrapper function clim_to_parquet()
# for admin level 0 (country level)


# Test that CHIRPS to parquet works
def test_chirps_to_parquet():
    """Test that a CHIRPS file has been created."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "chirps.parquet")
        clim2parquet.chirps_to_parquet("tests/test-data/country_A/", 0, file_path)

        # check that file is written and is a dataframe
        assert os.path.exists(file_path)


# Test that ERA5 mean temp. to parquet works
def test_era5mean_to_parquet():
    """Test that an ERA5-mean temp file has been created."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "era5mean.parquet")
        clim2parquet.era5mean_to_parquet("tests/test-data/country_A/", 0, file_path)

        # check that file is written and is a dataframe
        assert os.path.exists(file_path)


# Test that ERA5 min. temp. to parquet works
def test_era5min_to_parquet():
    """Test that an ERA5-min temp file has been created."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "era5min.parquet")
        clim2parquet.era5min_to_parquet("tests/test-data/country_A/", 0, file_path)

        # check that file is written and is a dataframe
        assert os.path.exists(file_path)


# Test that ERA5 max. temp. to parquet works
def test_era5max_to_parquet():
    """Test that an ERA5-max temp file has been created."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "era5max.parquet")
        clim2parquet.era5max_to_parquet("tests/test-data/country_A/", 0, file_path)

        # check that file is written and is a dataframe
        assert os.path.exists(file_path)


# Test that ERA5 relative humidity to parquet works
def test_era5rh_to_parquet():
    """Test that an ERA5-RH file has been created."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "era5rh.parquet")
        clim2parquet.era5rh_to_parquet("tests/test-data/country_A/", 0, file_path)

        # check that file is written and is a dataframe
        assert os.path.exists(file_path)


# Test that ERA5 specific humidity to parquet works
def test_era5sh_to_parquet():
    """Test that an ERA5-SH file has been created."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "era5sh.parquet")
        clim2parquet.era5sh_to_parquet("tests/test-data/country_A/", 0, file_path)

        # check that file is written and is a dataframe
        assert os.path.exists(file_path)


# Test that PERSIANN to parquet works
def test_persiann_to_parquet():
    """Test that a PERSIANN file has been created."""

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "persiann.parquet")
        clim2parquet.persiann_to_parquet("tests/test-data/country_A/", 0, file_path)

        # check that file is written and is a dataframe
        assert os.path.exists(file_path)


# Test that wrapper function for all data sources to parquet works
def test_clim_to_parquet():
    """Test that all climate data parquet files have been created."""

    admin_level = 0
    with tempfile.TemporaryDirectory() as temp_dir:
        # admin level defaults to 0
        clim2parquet.clim_to_parquet("tests/test-data/country_A/", temp_dir)

        file_names = [
            clim2parquet.tools._make_output_names("CHIRPS", admin_level),
            clim2parquet.tools._make_output_names("ERA5_mean", admin_level),
        ]

        for f in file_names:
            f = os.path.join(temp_dir, f)
            assert os.path.exists(f)


# Test that CHIRPS to parquet works for admin 1
def test_chirps_to_parquet_admin_1():
    """Test that all climate data parquet files have been created."""

    admin_level = 1

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = clim2parquet.tools._make_output_names("CHIRPS", 1)
        file_path = os.path.join(temp_dir, file_path)
        clim2parquet.chirps_to_parquet(
            "tests/test-data/country_A/", admin_level, file_path
        )

        # check that file is written and is a dataframe
        assert os.path.exists(file_path)
