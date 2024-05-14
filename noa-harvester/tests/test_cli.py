"""Testing cli module"""
from unittest.mock import patch

from click.testing import CliRunner

from noaharvester.cli import cli as main_cli

runner = CliRunner()


class TestCli:
    """Test class"""

    @patch("noaharvester.harvester.Harvester")
    # config_file is a pytest fixture in conftest.py
    def test_cli_describe(self, config_file):
        """Test cli describe"""
        response = runner.invoke(main_cli, ["describe", config_file.name])
        assert "Available parameters for selected collections" in response.output

    @patch("noaharvester.harvester.Harvester")
    def test_cli_query(self, config_file):
        """Test cli query"""
        response = runner.invoke(main_cli, ["query", config_file.name])
        assert "Querying providers for products" in response.output

    @patch("noaharvester.harvester.Harvester")
    def test_cli_download(self, config_file):
        """Test cli download"""
        response = runner.invoke(main_cli, ["download", config_file.name])
        assert "Downloading" in response.output

    # TODO assert all options in this test, so if someone adds a new option, will have
    #      to make a new test and add it here also
    def test_cli_no_option_selected(self, config_file):  # pylint:disable=unused-argument
        """Test cli with no option selected"""
        response = runner.invoke(main_cli, [""])
        assert "No such command" in response.output

    @patch("noaharvester.harvester.Harvester")
    def test_missing_config_argument(self, config_file):  # pylint:disable=unused-argument
        """Test cli without a necessary config argument"""
        message = "Missing argument 'CONFIG_FILE'"

        responses = []
        responses.append(runner.invoke(main_cli, ["download"]))
        responses.append(runner.invoke(main_cli, ["query"]))
        responses.append(runner.invoke(main_cli, ["describe"]))

        for response in responses:
            assert message in response.output
