"""Testing Harvester module"""

from unittest.mock import patch
from unittest.mock import Mock
from pathlib import Path
from noaharvester.harvester import Harvester


class TestHarvester:
    """Test class"""

    def test_harvester_constructor(self, config_file, output_folder):
        """Test constructor"""
        harvester = Harvester(config_file, output_path=output_folder)

        assert (
            harvester._search_items[0]["provider"] == "copernicus"  # pylint:disable=protected-access
        )
        assert (
            harvester._search_items[0]["collection"] == "Sentinel1"  # pylint:disable=protected-access
        )

        # Test constructor with shapefile:
        test_shape = "/test_data/foo_shape"
        test_shape_uri = Path(__file__).parent.name + test_shape
        harvester = Harvester(config_file, output_path=output_folder, shape_file=test_shape_uri)

        # Asserting that the extracted and transformed coordinates have successfully
        # entered in place of bbox of search terms
        assert (
            "24.139947" in harvester._search_items[0]["search_terms"]["box"]  # pylint:disable=protected-access
        )

    # The following tests are really dummy
    @patch("noaharvester.harvester.copernicus.Copernicus")
    @patch("noaharvester.harvester.earthdata.Earthdata")
    def test_query_data(
        self, mocked_earthdata, mocked_copernicus, config_file
    ):  # pylint:disable=unused-argument
        """Test query abstract call"""
        harvester = Harvester(config_file)
        harvester.query_data()

    @patch("noaharvester.harvester.copernicus.Copernicus")
    @patch("noaharvester.harvester.earthdata.Earthdata")
    def test_download_data(
        self, mocked_earthdata, mocked_copernicus, config_file
    ):  # pylint:disable=unused-argument
        """Test download abstract call"""
        harvester = Harvester(config_file)
        harvester.download_data()

    @patch("noaharvester.harvester.copernicus.Copernicus")
    @patch("noaharvester.harvester.earthdata.Earthdata")
    def test_describe(
        self, mocked_earthdata, mocked_copernicus, config_file
    ):  # pylint:disable=unused-argument
        """Test describe abstract call"""
        mocked_cp_class = Mock()
        mocked_cp_class.__class__.__name__ = "Copernicus"

        mocked_copernicus.return_value = mocked_cp_class

        harvester = Harvester(config_file)
        harvester.describe()

    @patch("noaharvester.harvester.copernicus.Copernicus")
    @patch("noaharvester.harvester.earthdata.Earthdata")
    def test_resolve_instances(
        self, mocked_earthdata, mocked_copernicus, config_file, output_folder
    ):  # pylint:disable=unused-argument
        """Test private function which resolves provider instance"""
        mocked_cp_class = Mock()
        mocked_cp_class.__class__.__name__ = "Copernicus"

        mocked_copernicus.return_value = mocked_cp_class

        harvester = Harvester(config_file, output_path=output_folder)
        harvester._resolve_provider_instance(  # pylint:disable=protected-access
            "earthdata"
        )
        harvester._resolve_provider_instance(  # pylint:disable=protected-access
            "earthsearch"
        )
        harvester._resolve_provider_instance(  # pylint:disable=protected-access
            "copernicus"
        )
        # adding it again to check the else clause
        harvester._resolve_provider_instance(   # pylint:disable=protected-access
            "copernicus"
        )

        assert "earthdata" in harvester._providers  # pylint:disable=protected-access
        assert "earthsearch" in harvester._providers  # pylint:disable=protected-access
        assert "copernicus" in harvester._providers  # pylint:disable=protected-access
