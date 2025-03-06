# SPDX-FileCopyrightText: 2025-present Imperial College of Science and Technology
#
# SPDX-License-Identifier: MIT

"""Test function to get data information from package data."""

import clim2parquet


def test_data_names() -> None:
    """Test that data names are returned."""
    data_names = clim2parquet.get_data_names()

    assert isinstance(data_names, list)
    assert len(data_names) == 7
