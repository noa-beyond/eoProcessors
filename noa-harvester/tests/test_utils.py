# TODO move these functions. Utils does not exist

# from noaharvester.utils import available_parameters, download_data


# config_file is a pytest fixture in conftest.py
def test_available_parameters(mocker, config_file, capfd):

    pass
    """
    mock_func = mocker.patch("cdsetool_cli.utils.describe_collection")
    mock_func.keys.return_value = "Sentinel2"
    available_parameters(config_file.name)
    out, _ = capfd.readouterr()
    assert "Sentinel2 available search terms" in out """


# config_file is a pytest fixture in conftest.py
def test_download_data(setenvvar, mocker, config_file, capfd):

    pass
    """
    mocker.patch("cdsetool_cli.utils.Credentials")
    mock_query_function = mocker.patch("cdsetool_cli.utils.query_features")
    mock_query_function.return_value = ["productId"]
    mock_download_function = mocker.patch("cdsetool_cli.utils.download_features")
    mock_download_function.return_value = ["list_of_downloaded_products", "1", "2"]
    mocker.patch("os.mkdir")
    download_data(config_file.name, True)
    out, _ = capfd.readouterr()
    assert "Available items for Sentinel2: 1" in out """
