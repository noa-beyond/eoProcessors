"""Testing Utils"""

from pathlib import Path
import pytest
from noaharvester import utils


class TestUtils:
    """Testing utils module"""

    def test_bbox_from_shp(self):
        """Testing bbox from shape"""

        area_base_name_path = "/test_data/foo_shape"
        result = utils.get_bbox_from_shp(
            Path(__file__).parent.name + area_base_name_path
        )

        # [22.657162600964472, 39.3802881819845, 22.929397652895076, 39.62149080384223]
        assert result[0] == pytest.approx(22.657162)
        assert result[1] == pytest.approx(39.380288)
        assert result[2] == pytest.approx(22.929397)
        assert result[3] == pytest.approx(39.621490)
