"""Pytest configuration module."""

import os
from unittest import mock
import pytest


CONTENT = (
    '[{"provider":"copernicus","collection":"Sentinel1","search_terms":{}},'
    '{"provider":"earthdata","collection":"MODIS","search_terms":{}}]'
)


@pytest.fixture(scope="function", autouse=True)
def mocked_collection_item():
    """Mocking an abstract collection item"""
    yield {
        "collection": "mocked_collection",
        "search_terms": {
            "box": "1.1, 2.2, 3.3, 4.4",
            "startDate": "mocked_start_date",
            "completionDate": "mocked_end_date",
            "short_name": "mocked_short_product_name",
        },
    }


@pytest.fixture(scope="function", autouse=True)
def config_file(tmp_path):
    """Mocked config file"""

    dir_path = tmp_path
    config_filename = dir_path / "config.json"
    config_filename.write_text(CONTENT)

    assert config_filename.read_text() == CONTENT
    yield config_filename


@pytest.fixture(scope="function", autouse=True)
def setenvvar(monkeypatch):
    """Fixture to mock the necessary environmental variables"""
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "COPERNICUS_LOGIN": "mocked_username",
            "COPERNICUS_PASSWORD": "mocked_password",
            "EARTHDATA_LOGIN": "mocked_username",
            "EARTHDATA_PASSWORD": "mocked_password",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield
