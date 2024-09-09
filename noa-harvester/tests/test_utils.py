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
            Path(__file__).parent.name + area_base_name_path,
            False
        )

        # [24.13994783117419, 38.638860630145174, 24.16899285922327, 38.66872082233962]
        assert result[0][0] == pytest.approx(24.139947)
        assert result[0][1] == pytest.approx(38.638860)
        assert result[0][2] == pytest.approx(24.168992)
        assert result[0][3] == pytest.approx(38.668720)

        # Checking if returns the same when bboxes = True:
        # TODO: separate tests for bboxes and multipolygons
        result = utils.get_bbox_from_shp(
            Path(__file__).parent.name + area_base_name_path,
            True
        )

        assert result[0][0] == pytest.approx(24.139947)
        assert result[0][1] == pytest.approx(38.638860)
        assert result[0][2] == pytest.approx(24.168992)
        assert result[0][3] == pytest.approx(38.668720)
