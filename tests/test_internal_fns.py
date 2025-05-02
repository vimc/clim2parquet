"""Test conversion from CSV to Parquet using top-level function."""

import pytest

import clim2parquet


def test_country_code() -> None:
    """Test that country code errors are caught."""
    gadm_version = "v410"
    cc = "XYZ"
    filename = f"{cc}_{gadm_version}"

    with pytest.raises(
        Exception, match=rf"Country code of {filename} not recog"
    ):
        clim2parquet.tools._get_country_code(filename, gadm_version)
    with pytest.raises(Exception, match=r"Country code not found in filename"):
        clim2parquet.tools._get_country_code(gadm_version, gadm_version)
