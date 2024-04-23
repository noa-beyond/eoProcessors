from click.testing import CliRunner

from noaharvester.cli import cli as main_cli

runner = CliRunner()


# config_file is a pytest fixture in conftest.py
def test_cli_describe(config_file, mocker):

    mocker.patch("noaharvester.harvester.Harvester")
    response = runner.invoke(main_cli, ["describe", config_file.name])
    assert "Available parameters for selected collections" in response.output


def test_cli_query(config_file, mocker):

    mocker.patch("noaharvester.harvester.Harvester")
    response = runner.invoke(main_cli, ["query", config_file.name])
    assert "Querying providers for products" in response.output


def test_cli_download(config_file, mocker):

    mocker.patch("noaharvester.harvester.Harvester")
    response = runner.invoke(main_cli, ["download", config_file.name])
    assert "Downloading" in response.output


# TODO assert all options in this test, so if someone adds a new option, will have
#      to make a new test and add it here also
# TODO add another test with config file present but no option, because it breaks
def test_cli_no_option_selected(config_file):

    response = runner.invoke(main_cli, [""])
    assert "No such command" in response.output
    print(response.output)


def test_missing_config_argument(config_file, mocker):

    message = "Missing argument 'CONFIG_FILE'"

    mocker.patch("noaharvester.harvester.Harvester")

    responses = []
    responses.append(runner.invoke(main_cli, ["download"]))
    responses.append(runner.invoke(main_cli, ["query"]))
    responses.append(runner.invoke(main_cli, ["describe"]))

    for response in responses:
        assert message in response.output
