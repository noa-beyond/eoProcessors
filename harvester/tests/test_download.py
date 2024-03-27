import unittest
import tempfile
from unittest.mock import patch, MagicMock

from harvester.download import (
    Sentinel2Request,
    getAccessToken,
    getAccessTokenViaRefreshToken,
    download_file,
)


class TestSentinel2Request(unittest.TestCase):
    def setUp(self):
        self.username = "test_user"
        self.password = "test_password"
        self.start_date = "2022-01-01"
        self.end_date = "2022-01-31"
        self.bbox = "10,20,30,40"
        self.tile = "T35SKC"
        self.cloud_cover = 50

    @patch("requests.post")
    def test_get_access_token_with_mock_post_request(self, mocked_post):
        mock_response = mocked_post.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "mocked_access_token",
            "refresh_token": "mocked_refresh_token",
        }

        response = getAccessToken(self.username, self.password)
        self.assertEqual(response[0], "mocked_access_token")
        self.assertEqual(response[1], "mocked_refresh_token")

    @patch("requests.post")
    def test_get_access_refresh_token_with_mock_post_request(self, mocked_post):
        mock_response = mocked_post.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "mocked_access_token"}

        response = getAccessTokenViaRefreshToken("mocked_refresh_token")
        self.assertEqual(response, "mocked_access_token")

    def test_queryData_with_bbox(self):
        request = Sentinel2Request(
            self.username,
            self.password,
            self.start_date,
            self.end_date,
            self.bbox,
            self.tile,
            self.cloud_cover,
        )
        expected_query = (
            "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?"
            "startDate=2022-01-01T00:00:00Z&"
            "completionDate=2022-01-31T23:59:59Z&"
            "maxRecords=1000&box=10.0,20.0,30.0,40.0&cloudCover=[0,50]"
        )
        self.assertEqual(request.queryData(), expected_query)

    def test_queryData_without_bbox(self):
        request = Sentinel2Request(
            self.username,
            self.password,
            self.start_date,
            self.end_date,
            None,
            self.tile,
            self.cloud_cover,
        )
        expected_query = (
            "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?"
            "startDate=2022-01-01T00:00:00Z&"
            "completionDate=2022-01-31T23:59:59Z&"
            "maxRecords=1000&cloudCover=[0,50]"
        )
        self.assertEqual(request.queryData(), expected_query)

    @patch("requests.Session.get")
    def test_search_success(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "id": "94faaa59-f4a3-4415-b490-6e75309872f2",
                    "properties": {
                        "thumbnail": "https://example.com/quicklook.jpg",
                        "title": "S2A_MSIL1C_20240103T092401_N0510_R093_T35SKC_20240103T101121.SAFE",
                        "startDate": "2022-01-01",
                        "cloudCover": 30,
                        "services": {
                            "download": {"url": "https://example.com/download.zip"}
                        },
                    },
                }
            ]
        }
        request = Sentinel2Request(
            self.username,
            self.password,
            self.start_date,
            self.end_date,
            self.bbox,
            self.tile,
            self.cloud_cover,
        )
        results = request.search()

        expected_results = {
            "S2A_MSIL1C_20240103T092401_N0510_R093_T35SKC_20240103T101121.SAFE": [
                "T35SKC",
                "2022-01-01",
                30,
                "https://example.com/quicklook.jpg",
                "https://example.com/download.zip",
                "94faaa59-f4a3-4415-b490-6e75309872f2",
            ]
        }
        self.assertEqual(results, expected_results)

    @patch("requests.Session.get")
    def test_search_failure(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 500
        mock_response.reason = "Mocked reasons"
        request = Sentinel2Request(
            self.username,
            self.password,
            self.start_date,
            self.end_date,
            self.bbox,
            self.tile,
            self.cloud_cover,
        )

        expected_message = (
            f"Failed to retrieve data from the server. Status code: {mock_response.status_code} \n"
            f"Message: {mock_response.reason}"
        )

        with self.assertLogs(level="ERROR") as cm:
            results = request.search()
        self.assertEqual(cm.output, [f"ERROR:harvester:{expected_message}"])
        self.assertIsNone(results)

    def test_download_file_fails_and_error_is_logged(self):

        # Given
        content_length = 8  # Magic Number
        mocked_session = MagicMock()
        mocked_response = MagicMock()

        mocked_response.headers = {"content-length": content_length}

        # Creating a list total length of 6
        mocked_response.iter_content.return_value = [
            bytearray("500", encoding="utf-8"),
            bytearray("500", encoding="utf-8"),
        ]
        mocked_session.get.return_value.__enter__.return_value = mocked_response

        with tempfile.NamedTemporaryFile() as tmp_file:
            expected_message = f"ERROR, something went wrong downloading {tmp_file.name}. {content_length}/6"

            with self.assertLogs(level="ERROR") as cm:
                download_file(mocked_session, "mocked_url", tmp_file.name)
            self.assertEqual(cm.output, [f"ERROR:harvester:{expected_message}"])

    def test_download_file_succeeds(self):

        # Given
        content_length = 6  # Magic Number
        mocked_session = MagicMock()
        mocked_response = MagicMock()

        mocked_response.headers = {"content-length": content_length}
        mocked_response.iter_content.return_value = [
            bytearray("500", encoding="utf-8"),
            bytearray("500", encoding="utf-8"),
        ]
        mocked_session.get.return_value.__enter__.return_value = mocked_response

        with tempfile.NamedTemporaryFile() as tmp_file:

            download_file(mocked_session, "mocked_url", tmp_file.name)
            tmp_file.seek(0, 2)
            self.assertEqual(tmp_file.tell(), content_length)

    # TODO this is an integration test and it is not correct to be with unit tests
    # Commented in order to be corrected included in another place
    """     @patch.dict(os.environ, {"outDir": "mockedDownload"})
        @patch("requests.get")
        def test_download(self, mock_get):
            mock_response = mock_get.return_value
            mock_response.content = b"Mock content"
            request = Sentinel2Request(
                self.username,
                self.password,
                self.start_date,
                self.end_date,
                self.bbox,
                self.tile,
                self.cloud_cover,
            )
            results = {
                "S2A_MSIL1C_20240103T092401_N0510_R093_T35SKC_20240103T101121.SAFE": [
                    "T35SKC",
                    "2022-01-01",
                    30,
                    "https://example.com/quicklook.jpg",
                    "https://example.com/download.zip",
                ]
            }
            request.download(results)
            filename = "20220101_123_quicklook.jpg"
            mock_get.assert_called_once_with("https://example.com/quicklook.jpg")
            mock_response.assert_called_once_with(filename, "wb")
            mock_response.write.assert_called_once_with(b"Mock content")
    """


if __name__ == "__main__":
    unittest.main()
