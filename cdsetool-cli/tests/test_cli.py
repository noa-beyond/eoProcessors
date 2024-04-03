import pytest
from click.testing import CliRunner

# from cli import available_parameters  # , download_data
from cdsetool_cli.cli import cli as main_cli

runner = CliRunner()

CONTENT = "{'Sentinel2': 'None'}"


@pytest.fixture
def config_file(tmp_path):

    dir_path = tmp_path / "config"
    dir_path.mkdir()
    config_file = dir_path / "config.json"
    config_file.write_text(CONTENT)

    assert config_file.read_text() == CONTENT
    return config_file


def test_cli_parameters(config_file, mocker):

    mock_func = mocker.patch("cdsetool_cli.utils.describe_collection")
    mock_func.keys.return_value = "Sentinel2"
    response = runner.invoke(main_cli, ["-p", config_file.name])
    assert "Sentinel2" in response.output
    print(response.output)


# TODO assert all options in this test, so if someone adds a new option, will have
#      to make a new test and add it here also
# TODO add another test with config file present but no option, because it breaks
def test_cli_no_option_selected(config_file):

    response = runner.invoke(main_cli, [""])
    assert "Please select at least one option" in response.output
    print(response.output)
