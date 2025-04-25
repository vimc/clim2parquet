"""Test that admin unit UID maker works."""

from pathlib import Path

import pandas as pd
import pytest

import clim2parquet


def test_admin_unit_uid_maker() -> None:
    """Test that the admin unit UID maker works."""
    path_from = Path("tests/test-data/ZZZ/")
    data = clim2parquet.make_admin_unit_ids(path_from)

    col_names = [
        f"admin_unit_{i}" for i in clim2parquet.tools._gadm_levels()
    ] + ["uid"]

    assert isinstance(data, pd.DataFrame)
    assert all(i in data.columns for i in col_names)


def test_admin_unit_uid_errors() -> None:
    """Test error from admin unit UID maker."""
    path_from_bad = Path("tests/test-data/ABC/")
    with pytest.raises(Exception, match=r"not found or is not a directory"):
        clim2parquet.make_admin_unit_ids(path_from_bad)
