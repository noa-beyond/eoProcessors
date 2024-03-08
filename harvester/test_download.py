import unittest
from unittest.mock import patch
from download import Sentinel2Request

class TestSentinel2Request(unittest.TestCase):
    def setUp(self):
        self.username = "test_user"
        self.password = "test_password"
        self.start_date = "2022-01-01"
        self.end_date = "2022-01-31"
        self.bbox = [10, 20, 30, 40]
        self.tile = "T123"
        self.cloud_cover = 50

    def test_queryData_with_bbox(self):
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, self.bbox, self.tile, self.cloud_cover)
        expected_query = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?startDate=2022-01-01T00:00:00Z&completionDate=2022-01-31T23:59:59Z&maxRecords=1000&box=10,20,30,40&cloudCover=[0,50]"
        self.assertEqual(request.queryData(), expected_query)

    def test_queryData_without_bbox(self):
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, None, self.tile, self.cloud_cover)
        expected_query = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?startDate=2022-01-01T00:00:00Z&completionDate=2022-01-31T23:59:59Z&maxRecords=1000&cloudCover=[0,50]"
        self.assertEqual(request.queryData(), expected_query)

    @patch('requests.Session.get')
    def test_search_success(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "properties": {
                        "thumbnail": "https://example.com/quicklook.jpg",
                        "title": "Sentinel2_20220101_T123",
                        "startDate": "2022-01-01",
                        "cloudCover": 30,
                        "services": {
                            "download": {
                                "url": "https://example.com/download.zip"
                            }
                        }
                    }
                }
            ]
        }
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, self.bbox, self.tile, self.cloud_cover)
        results = request.search()
        expected_results = {
            "Sentinel2_20220101_T123": ["123", "2022-01-01", 30, "https://example.com/quicklook.jpg", "https://example.com/download.zip"]
        }
        self.assertEqual(results, expected_results)

    @patch('requests.Session.get')
    def test_search_failure(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 500
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, self.bbox, self.tile, self.cloud_cover)
        results = request.search()
        self.assertIsNone(results)

if __name__ == '__main__':
    unittest.main()

import unittest
from unittest.mock import patch
from download import Sentinel2Request

class TestSentinel2Request(unittest.TestCase):
    def setUp(self):
        self.username = "test_user"
        self.password = "test_password"
        self.start_date = "2022-01-01"
        self.end_date = "2022-01-31"
        self.bbox = [10, 20, 30, 40]
        self.tile = "T123"
        self.cloud_cover = 50

    def test_queryData_with_bbox(self):
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, self.bbox, self.tile, self.cloud_cover)
        expected_query = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?startDate=2022-01-01T00:00:00Z&completionDate=2022-01-31T23:59:59Z&maxRecords=1000&box=10,20,30,40&cloudCover=[0,50]"
        self.assertEqual(request.queryData(), expected_query)

    def test_queryData_without_bbox(self):
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, None, self.tile, self.cloud_cover)
        expected_query = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?startDate=2022-01-01T00:00:00Z&completionDate=2022-01-31T23:59:59Z&maxRecords=1000&cloudCover=[0,50]"
        self.assertEqual(request.queryData(), expected_query)

    @patch('requests.Session.get')
    def test_search_success(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "features": [
                {
                    "properties": {
                        "thumbnail": "https://example.com/quicklook.jpg",
                        "title": "Sentinel2_20220101_T123",
                        "startDate": "2022-01-01",
                        "cloudCover": 30,
                        "services": {
                            "download": {
                                "url": "https://example.com/download.zip"
                            }
                        }
                    }
                }
            ]
        }
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, self.bbox, self.tile, self.cloud_cover)
        results = request.search()
        expected_results = {
            "Sentinel2_20220101_T123": ["123", "2022-01-01", 30, "https://example.com/quicklook.jpg", "https://example.com/download.zip"]
        }
        self.assertEqual(results, expected_results)

    @patch('requests.Session.get')
    def test_search_failure(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.status_code = 500
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, self.bbox, self.tile, self.cloud_cover)
        results = request.search()
        self.assertIsNone(results)

    @patch('requests.get')
    def test_download(self, mock_get):
        mock_response = mock_get.return_value
        mock_response.content = b"Mock content"
        request = Sentinel2Request(self.username, self.password, self.start_date, self.end_date, self.bbox, self.tile, self.cloud_cover)
        results = {
            "Sentinel2_20220101_T123": ["123", "2022-01-01", 30, "https://example.com/quicklook.jpg", "https://example.com/download.zip"]
        }
        request.download(results)
        filename = "20220101_123_quicklook.jpg"
        mock_get.assert_called_once_with("https://example.com/quicklook.jpg")
        mock_response.assert_called_once_with(filename, 'wb')
        mock_response.write.assert_called_once_with(b"Mock content")

if __name__ == '__main__':
    unittest.main()