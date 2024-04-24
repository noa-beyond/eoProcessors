from unittest.mock import patch
from unittest.mock import Mock
from noaharvester.harvester import Harvester


class TestHarvester:

    def test_harvester_constructor(self, config_file):

        harvester = Harvester(config_file)

        assert harvester._search_items[0]["provider"] == "copernicus"
        assert harvester._search_items[0]["collection"] == "Sentinel1"

    # The following tests are really dummy
    @patch("noaharvester.harvester.copernicus.Copernicus")
    @patch("noaharvester.harvester.earthdata.Earthdata")
    def test_query_data(self, mocked_earthdata, mocked_copernicus, config_file):

        harvester = Harvester(config_file)
        harvester.query_data()

    @patch("noaharvester.harvester.copernicus.Copernicus")
    @patch("noaharvester.harvester.earthdata.Earthdata")
    def test_download_data(self, mocked_earthdata, mocked_copernicus, config_file):

        harvester = Harvester(config_file)
        harvester.download_data()

    @patch("noaharvester.harvester.copernicus.Copernicus")
    @patch("noaharvester.harvester.earthdata.Earthdata")
    def test_describe(self, mocked_earthdata, mocked_copernicus, config_file):

        mocked_cp_class = Mock()
        mocked_cp_class.__class__.__name__ = "Copernicus"

        mocked_copernicus.return_value = mocked_cp_class

        harvester = Harvester(config_file)
        harvester.describe()
