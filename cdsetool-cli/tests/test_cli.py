from click.testing import CliRunner

from cdsetool_cli.cli import cli as main_cli

runner = CliRunner()


# config_file is a pytest fixture in conftest.py
def test_cli_parameters(config_file, mocker):

    mocker.patch("cdsetool_cli.cli.available_parameters")
    response = runner.invoke(main_cli, ["-p", config_file.name])
    assert "Available parameters per data source" in response.output


def test_cli_download(config_file, mocker):

    mocker.patch("cdsetool_cli.cli.download_data")
    response = runner.invoke(main_cli, ["-d", config_file.name])
    assert "Downloading" in response.output


# TODO assert all options in this test, so if someone adds a new option, will have
#      to make a new test and add it here also
# TODO add another test with config file present but no option, because it breaks
def test_cli_no_option_selected(config_file):

    response = runner.invoke(main_cli, [""])
    assert "Please select at least one option" in response.output
    print(response.output)
