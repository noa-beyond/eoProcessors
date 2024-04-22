import os
import pytest

from unittest import mock


CONTENT = "{'Sentinel2':{'productId':'mockedId'} }"


@pytest.fixture(scope="function", autouse=True)
def config_file(tmp_path):

    dir_path = tmp_path / "config"
    dir_path.mkdir()
    config_filename = dir_path / "config.json"
    config_filename.write_text(CONTENT)

    assert config_filename.read_text() == CONTENT
    return config_filename


@pytest.fixture(scope="function", autouse=True)
def setenvvar(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        envvars = {
            "login": "mocked_username",
            "password": "mocked_password",
        }
        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield
