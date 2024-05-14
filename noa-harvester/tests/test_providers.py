"""Testing providers"""
from unittest.mock import patch
from pathlib import Path
import pytest

from noaharvester.providers import DataProvider
from noaharvester.providers.earthdata import Earthdata
from noaharvester.providers.copernicus import Copernicus


class TestProviders:
    """Testing class"""

    def test_data_provider(self):
        """Testing ABC class"""
        class AProvider(DataProvider):
            """Mocked Instance"""
            def download(self, item):
                pass

            def query(self, item):
                pass

            def describe(self, collection):
                pass

        mocked_data_provider = AProvider()

        mocked_data_provider.download(None)
        mocked_data_provider.query(None)
        mocked_data_provider.describe(None)

        assert mocked_data_provider._download_path == Path("./data").absolute()  # pylint:disable=protected-access

    @patch("noaharvester.providers.earthdata.earthaccess.login")
    @patch("noaharvester.providers.earthdata.earthaccess.search_data")
    def test_earthdata_query(
        self, mocked_search, mocked_login, mocked_collection_item
    ):  # pylint:disable=unused-argument
        """Testing earthdata query"""
        mocked_query_results = ["a result", "a second result"]
        mocked_search.return_value = mocked_query_results

        earthdata = Earthdata()
        result = earthdata.query(mocked_collection_item)

        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == len(mocked_query_results)

    @patch("noaharvester.providers.earthdata.earthaccess.login")
    @patch("noaharvester.providers.earthdata.earthaccess.download")
    @patch("noaharvester.providers.earthdata.earthaccess.search_data")
    def test_earthdata_download(
        self, mocked_search, mocked_download, mocked_login, mocked_collection_item
    ):   # pylint:disable=unused-argument
        """Testing earthdata download"""
        mocked_query_results = ["a result", "a second result"]
        mocked_search.return_value = mocked_query_results

        earthdata = Earthdata()
        result = earthdata.download(mocked_collection_item)

        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == len(mocked_query_results)

    @patch("noaharvester.providers.earthdata.earthaccess.login")
    def test_earthdata_describe_raises_not_implemented(self, mocked_earthaccess):  # pylint:disable=unused-argument
        """Testing not implemented error raise"""
        earthdata = Earthdata()
        with pytest.raises(NotImplementedError):
            earthdata.describe(None)

    @patch("noaharvester.providers.copernicus.query_features")
    @patch("noaharvester.providers.copernicus.Credentials")
    def test_copernicus_query(
        self, mock_credentials_constructor, mocked_search, mocked_collection_item
    ):
        """Testing Copernicus query"""
        mock_credentials_constructor.return_value = "a mocked class"
        mocked_query_results = ["a result", "a second result"]
        mocked_search.return_value = mocked_query_results

        copernicus = Copernicus()
        result = copernicus.query(mocked_collection_item)

        assert copernicus.credentials == "a mocked class"
        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == len(mocked_query_results)

    @patch("noaharvester.providers.copernicus.download_features")
    @patch("noaharvester.providers.copernicus.query_features")
    @patch("noaharvester.providers.copernicus.Credentials")
    def test_copernicus_download(
        self,
        mock_credentials_constructor,
        mocked_search,
        mocked_download,
        mocked_collection_item,
    ):
        """Testing Copernicus download"""
        mock_credentials_constructor.return_value = "a mocked class"
        mocked_query_results = ["a result", "a second result"]
        mocked_search.return_value = mocked_query_results
        mocked_download.return_value = mocked_query_results

        copernicus = Copernicus()
        result = copernicus.download(mocked_collection_item)

        assert copernicus.credentials == "a mocked class"
        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == len(mocked_query_results)

    @patch("noaharvester.providers.copernicus.describe_collection")
    @patch("noaharvester.providers.copernicus.Credentials")
    def test_copernicus_describe(
        self, mock_credentials_constructor, mocked_describe
    ):  # pylint:disable=unused-argument
        """Testing Copernicus describe"""
        mocked_describe.return_value = {"a key": "a value"}

        copernicus = Copernicus()
        result = copernicus.describe("mocked_collection_name")

        assert result[0] == "mocked_collection_name"
        assert result[1] == ["a key"]
