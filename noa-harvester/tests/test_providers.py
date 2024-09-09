"""Testing providers"""

import pytest
# import logging
from unittest.mock import patch, Mock, mock_open
from pathlib import Path


from noaharvester.providers import DataProvider
from noaharvester.providers.earthdata import Earthdata
from noaharvester.providers.earthsearch import Earthsearch
from noaharvester.providers.copernicus import Copernicus


class TestProviders:
    """Testing class"""

    def test_data_provider(self, output_folder):
        """Testing ABC class"""

        class AProvider(DataProvider):
            """Mocked Instance"""

            def download(self, item):
                pass

            def query(self, item):
                pass

            def describe(self, collection):
                pass

        mocked_data_provider = AProvider(output_folder)

        mocked_data_provider.download(None)
        mocked_data_provider.query(None)
        mocked_data_provider.describe(None)

        assert (
            mocked_data_provider._download_path == Path("./data").absolute()   # pylint:disable=protected-access
        )

    @patch("noaharvester.providers.earthdata.earthaccess.login")
    @patch("noaharvester.providers.earthdata.earthaccess.search_data")
    def test_earthdata_query(
        self, mocked_search, mocked_login, mocked_collection_item, output_folder
    ):  # pylint:disable=unused-argument
        """Testing earthdata query"""
        mocked_query_results = ["a result", "a second result"]
        mocked_search.return_value = mocked_query_results

        earthdata = Earthdata(output_folder)
        result = earthdata.query(mocked_collection_item)

        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == len(mocked_query_results)

    @patch("noaharvester.providers.earthdata.earthaccess.login")
    @patch("noaharvester.providers.earthdata.earthaccess.download")
    @patch("noaharvester.providers.earthdata.earthaccess.search_data")
    def test_earthdata_download(
        self, mocked_search, mocked_download, mocked_login, mocked_collection_item, output_folder
    ):  # pylint:disable=unused-argument
        """Testing earthdata download"""
        mocked_single = Mock()
        mocked_single.data_links.return_value = True
        mocked_query_results = [mocked_single]
        mocked_search.return_value = mocked_query_results

        earthdata = Earthdata(output_folder)
        result = earthdata.download(mocked_collection_item)

        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == len(mocked_query_results)

    @patch("noaharvester.providers.earthdata.earthaccess.login")
    def test_earthdata_describe_raises_not_implemented(
        self, mocked_earthaccess, output_folder
    ):  # pylint:disable=unused-argument
        """Testing not implemented error raise"""
        earthdata = Earthdata(output_folder)
        with pytest.raises(NotImplementedError):
            earthdata.describe(None)

    @patch("noaharvester.providers.copernicus.query_features")
    @patch("noaharvester.providers.copernicus.Credentials")
    def test_copernicus_query(
        self, mock_credentials_constructor, mocked_search, mocked_collection_item, output_folder
    ):
        """Testing Copernicus query"""
        mock_credentials_constructor.return_value = "a mocked class"
        mocked_query_results = ["a result", "a second result"]
        mocked_search.return_value = mocked_query_results

        copernicus = Copernicus(output_folder)
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
        output_folder
    ):
        """Testing Copernicus download"""
        mocked_single = {"id": True, "properties": {"status": "ONLINE"}}
        mock_credentials_constructor.return_value = "a mocked class"
        mocked_query_results = [mocked_single]
        mocked_search.return_value = mocked_query_results
        mocked_download.return_value = mocked_query_results

        copernicus = Copernicus(output_folder)
        result = copernicus.download(mocked_collection_item)

        assert copernicus.credentials == "a mocked class"
        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == len(mocked_query_results)

    @patch("noaharvester.providers.copernicus.describe_collection")
    @patch("noaharvester.providers.copernicus.Credentials")
    def test_copernicus_describe(
        self, mock_credentials_constructor, mocked_describe, output_folder
    ):  # pylint:disable=unused-argument
        """Testing Copernicus describe"""
        mocked_describe.return_value = {"a key": "a value"}

        copernicus = Copernicus(output_folder)
        result = copernicus.describe("mocked_collection_name")

        assert result[0] == "mocked_collection_name"
        assert result[1] == ["a key"]

    @patch("noaharvester.providers.earthsearch.pystac_client.Client")
    def test_earthsearch_query(
        self, mocked_pystac_client, mocked_collection_item, output_folder
    ):  # pylint:disable=unused-argument
        """Testing earthsearch query"""

        # TODO the same "fixture" exists in download. Put it in conftest
        # or in this test class
        class MockedResults(Mock):
            def __init__(self):
                super().__init__()
                self.items = ["a result", "a second result"]

        # When mocking STAC catalog "Open", "Search" and "results"
        mocked_catalog_open = Mock()
        mocked_catalog_search = Mock()
        results = MockedResults()

        # We go backwards to mock each step:
        # 1) when the return of our mocked item collection results...
        mocked_catalog_search.item_collection.return_value = results
        # 2) is part of the collection which we searched for...
        mocked_catalog_open.search.return_value = mocked_catalog_search
        # 3) we have started by opening the Catalog:
        mocked_pystac_client.open.return_value = mocked_catalog_open

        earthsearch = Earthsearch(output_folder)
        result = earthsearch.query(mocked_collection_item)

        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == len(results.items)

    @patch("noaharvester.providers.earthsearch.pystac_client.Client")
    @patch("noaharvester.providers.earthsearch.shutil")
    @patch("noaharvester.providers.earthsearch.requests")
    def test_earthsearch_download(
        self,
        mock_requests,
        mock_shutil,
        mocked_pystac_client,
        mocked_collection_item,
        output_folder,
        caplog
    ):  # pylint:disable=unused-argument

        class MockedAsset(Mock):
            def __init__(self):
                super().__init__()
                self.href = "https://Mocked_URI/asset.tif"

        class MockedResult(Mock):
            def __init__(self):
                super().__init__()
                self.id = "Mocked Id"
                self.assets = {"visual": MockedAsset()}

        class MockedResults(Mock):
            def __init__(self):
                super().__init__()
                a_result = MockedResult()
                self.items = [a_result]

        class MockedRequestResponse(Mock):
            def __init__(self, status):
                super().__init__()
                self.status_code = status

        mock_requests.get.return_value = MockedRequestResponse(200)
        mock_shutil.copyfileobj.return_value = True

        # When mocking STAC catalog "Open", "Search" and "results"
        mocked_catalog_open = Mock()
        mocked_catalog_search = Mock()
        results = MockedResults()

        # We go backwards to mock each step:
        # 1) when the return of our mocked item collection results...
        mocked_catalog_search.item_collection.return_value = results
        # 2) is part of the collection which we searched for...
        mocked_catalog_open.search.return_value = mocked_catalog_search
        # 3) we have started by opening the Catalog:
        mocked_pystac_client.open.return_value = mocked_catalog_open

        earthsearch = Earthsearch(output_folder)

        with patch("builtins.open", mock_open()) as _:
            result = earthsearch.download(mocked_collection_item)

        assert result[0] == mocked_collection_item["collection"]
        assert result[1] == 1

        # TODO fix this test
        # mock_requests.get.return_value = MockedRequestResponse(404)
        # with caplog.at_level(logging.ERROR):
        #     earthsearch.download(mocked_collection_item)
        # assert "Failed" in caplog.text

    def test_earthsearch_describe_raises_not_implemented(
        self, output_folder
    ):  # pylint:disable=unused-argument
        """Testing not implemented error raise"""
        earthsearch = Earthsearch(output_folder)
        with pytest.raises(NotImplementedError):
            earthsearch.describe(None)
