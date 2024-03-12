import logging
import os
# from pathlib import Path
import requests
import time
# from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


class SatelliteSensorRequest:

    def __init__(
        self, username, password, start_date, end_date, bbox, tile=None
    ) -> None:
        """
        Initialize the Download class.

        Parameters:
        start_date (str): The start date of the data to be downloaded.
        end_date (str): The end date of the data to be downloaded.
        bbox (list): The bounding box coordinates in the format [min_lon, min_lat, max_lon, max_lat].
        tile (str, optional): The tile identifier. Defaults to None.

        Raises:
        Exception: If spatial parameters are incorrect. Bounding box must have 4 coordinates or tile must be provided.
        """
        self.username = username
        self.password = password
        self.startDate = start_date
        self.endDate = end_date
        if bbox:
            self.bbox = string_bbox_to_list(bbox)
            if not self.check_bbox_length() or not self.check_bbox_validity():
                raise Exception(
                    """
                        Spatial Parameters are non-correct: Bounding box coordinates are not valid.
                        [min_lon, min_lat, max_lon, max_lat] are required.
                    """
                )
        elif tile is None:
            raise Exception("Spatial Parameters must be provided!")
        else:
            self.bbox = None
            self.tile = tile

    # TODO: is this check correct? (when "forming" a bbox, is the < relation between min and max correct?)
    def check_bbox_validity(self):
        """
        Checks if the bounding box coordinates are valid.

        Returns:
            bool: True if the bounding box coordinates are valid, False otherwise.
        """
        if self.bbox:
            if self.bbox[0] < self.bbox[2] and self.bbox[1] < self.bbox[3]:
                return True
        return False

    def check_bbox_length(self):
        """
        Checks if the bounding box coordinates are valid.

        Returns:
            bool: True if the bounding box coordinates are valid, False otherwise.
        """
        if self.bbox:
            if len(self.bbox) == 4:
                return True
        return False

    def split_bbox(self):
        """
        Splits the bounding box coordinates into individual variables.

        Returns:
            Tuple: A tuple containing the xmin, ymin, xmax, and ymax values of the bounding box.
        """
        xmin, ymin, xmax, ymax = self.bbox[0], self.bbox[1], self.bbox[2], self.bbox[3]
        return xmin, ymin, xmax, ymax

    def getAccessToken(self):
        url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

        # Headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # Data payload
        data = {
            "grant_type": "password",
            "username": self.username,  # Replace <LOGIN> with your actual login username
            "password": self.password,  # Replace <PASSWORD> with your actual password
            "client_id": "cdse-public",
        }

        # Make the POST request
        response = requests.post(url, headers=headers, data=data)
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            json_response = response.json()

            # Extract the access token
            access_token = json_response.get("access_token", None)
            refresh_token = json_response.get("refresh_token", None)
            # Print the access token
            if access_token and refresh_token:
                return access_token, refresh_token
        else:
            logging.error(
                f"Failed to retrieve data from the server. Status code: {response.status_code} \n"
                f"Message: {response.reason}"
            )
        return None

    def getAccessTokenViaRefreshToken(self, refresh_token):
        url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

        # Headers
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # Data payload
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,  # Replace <REFRESH_TOKEN> with your actual refresh token
            "client_id": "cdse-public",
        }

        # Make the POST request
        response = requests.post(url, headers=headers, data=data)

        # Check the response
        if response.status_code == 200:
            json_response = response.json()
            # Extract the access token
            access_token = json_response.get("access_token", None)
        else:
            print(
                f"Failed to retrieve the access token. Status code: {response.status_code}"
            )
        return access_token


class Sentinel2Request(SatelliteSensorRequest):
    def __init__(
        self, username, password, start_date, end_date, bbox, tile, cloud_cover=100
    ) -> None:
        super().__init__(username, password, start_date, end_date, bbox, tile)
        cloud_cover = int(cloud_cover)
        if cloud_cover < 0 or cloud_cover > 100:
            raise Exception
        self.cloud_cover = cloud_cover
        self.query = self.queryData()  # Fix: Added 'self.' before 'queryData()'

    def queryData(self):
        """
        Constructs and returns the query URL based on the provided parameters.

        Returns:
            str: The query URL.
        """
        if self.bbox:
            xmin, ymin, xmax, ymax = self.split_bbox()
            return f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?"\
                f"startDate={self.startDate}T00:00:00Z&"\
                f"completionDate={self.endDate}T23:59:59Z&"\
                f"maxRecords=1000&box={xmin},{ymin},{xmax},{ymax}&"\
                f"cloudCover=[0,{self.cloud_cover}]"
        else:
            return f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?"\
                f"startDate={self.startDate}T00:00:00Z&"\
                f"completionDate={self.endDate}T23:59:59Z&"\
                f"maxRecords=1000&"\
                f"cloudCover=[0,{self.cloud_cover}]"

    def search(self):
        """
        Searches for data based on the provided query.

        Returns:
            dict: A dictionary containing the search results, where the keys are identifiers and the values are lists
                  containing the selected tile, begin date, cloud coverage, and quicklook URL.
        """
        session = requests.Session()
        session.stream = True

        # print(self.query)

        response = session.get(self.query)
        if not response.status_code == 200:
            logging.error(
                f"Failed to retrieve data from the server. Status code: {response.status_code} \n"
                f"Message: {response.reason}"
            )
            return None
        results = {}
        response = response.json()
        for feature in response["features"]:
            try:
                quicklook = feature["properties"]["thumbnail"]
                identifier = feature["properties"]["title"]
                if level == "1" and "L2A" in identifier.split("_")[1]:
                    continue
                if "L2A" in identifier.split("_")[1] and level == "1":
                    continue
                selected_tile = feature["properties"]["title"].split("_")[5][1:]
                beginDate = feature["properties"]["startDate"]
                cloud_coverage = feature["properties"]["cloudCover"]
                download_url = feature["properties"]["services"]["download"]["url"]
                id = feature["id"]
                results[identifier] = [
                    selected_tile,
                    beginDate,
                    cloud_coverage,
                    quicklook,
                    download_url,
                    id,
                ]
                # TODO: Should we handle this exception, even log-wise?
            except Exception:
                continue
        return results

    def download(self, results):
        # showResultsAsTable(results)
        logging.info("Searching results:")
        time.sleep(10)
        logging.info("Downloading results:")

        # Retrieve the output directory path from the environment variable
        output_dir = os.getenv("outDir", "/app/data/")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        access_token, refresh_token = self.getAccessToken()

        if access_token is None:
            raise Exception("Failed to retrieve the access token.")

        access_token = self.getAccessTokenViaRefreshToken(refresh_token)

        print(f"Access Token: {access_token}")
        for identifier, values in results.items():
            filename = os.path.join(output_dir, f"{identifier}.zip")
            headers = {"Authorization": f"Bearer {access_token}"}
            url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({values[-1]})/$value"
            print(url)
            headers = {"Authorization": f"Bearer {access_token}"}

            session = requests.Session()
            session.headers.update(headers)
            response = session.get(url, headers=headers, stream=True)

            with open(filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)

            logging.info(f"Downloaded {filename}")

    def download_file(self, session, url, filename):
        with session.get(url, stream=True) as response:
            response.raise_for_status()  # This will raise an error for non-200 responses
            total_size_in_bytes = int(response.headers.get("content-length", 0))
            block_size = 8192  # 8 Kilobytes

            progress_bar = tqdm(
                total=total_size_in_bytes, unit="iB", unit_scale=True, desc=filename
            )
            with open(filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=block_size):
                    progress_bar.update(len(chunk))
                    file.write(chunk)
            progress_bar.close()

            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                logging.error(f"ERROR, something went wrong downloading {filename}")

    def download_files_concurrently(self, results, output_dir, access_token):
        headers = {"Authorization": f"Bearer {access_token}"}
        session = requests.Session()
        session.headers.update(headers)

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for identifier, values in results.items():
                filename = os.path.join(output_dir, f"{identifier}.zip")
                url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({values[-1]})/$value"
                futures.append(
                    executor.submit(self.download_file, session, url, filename)
                )

            # Wait for all futures to complete
            for future in as_completed(futures):
                future.result()  # This will re-raise any exceptions caught during the download process


# Additional functions
def string_bbox_to_list(bbox):
    """
    Converts the bounding box coordinates from string to list.

    Returns:
        list: The bounding box coordinates in list format.
    """
    if bbox:
        return [float(x) for x in bbox.split(",")]
    return None


def getParametersFromDockerEnv():
    """
    Get the parameters from the Docker environment variables.

    Returns:
        dict: The parameters.
    """
    parameters = {}
    if "username" in os.environ:
        parameters["username"] = os.environ["username"]
    if "password" in os.environ:
        parameters["password"] = os.environ["password"]
    if "startDate" in os.environ:
        parameters["startDate"] = os.environ["startDate"]
    if "endDate" in os.environ:
        parameters["endDate"] = os.environ["endDate"]
    if "bbox" in os.environ:
        parameters["bbox"] = os.environ["bbox"]
    else:
        parameters["bbox"] = None
    if "tile" in os.environ:
        parameters["tile"] = os.environ["tile"]
    else:
        parameters["tile"] = None
    if "cloudCover" in os.environ:
        parameters["cloudCover"] = os.environ["cloudCover"]
    if "level" in os.environ:
        parameters["level"] = os.environ["level"]
    return parameters


def showResultsAsTable(results):
    """
    Show the results as a table.

    Parameters:
    results (dict): The results.
    """
    print("Tile\tBegin Date\tCloud Coverage\tDownload URL")
    for identifier, values in results.items():
        print(f"{values[0]}\t{values[1]}\t{values[2]}\t{values[-1]}")


def checkParameters(satellite, parameters):
    """
    Check if the parameters are correct.

    Parameters:
    satellite (str): The satellite sensor.
    parameters (dict): The parameters.

    Returns:
        bool: True if the parameters are correct, False otherwise.
    """
    if satellite == "Sentinel2":
        if (
            "level" in parameters
            and "username" in parameters
            and "password" in parameters
            and "startDate" in parameters
            and "endDate" in parameters
            and ("bbox" in parameters or "tile" in parameters)
            and "cloudCover" in parameters
        ):
            return True
    return False


if __name__ == "__main__":
    if not checkParameters("Sentinel2", getParametersFromDockerEnv()):
        SystemError("Incorrect or/and insufficient parameters")

    username, password, startDate, endDate, bbox, tile, cloudCover, level = (
        getParametersFromDockerEnv().values()
    )

    if str(level) not in ["1", "2", "12"]:
        raise Exception("Incorrect level parameter. It must be 1, 2 or 12")

    # TODO introduce the level. It was introduced but the constructor cannot receive it
    s2 = Sentinel2Request(
        username, password, startDate, endDate, bbox, tile, cloudCover
    )
    s2_results = s2.search()
    print(f"# of products found:{len(s2_results)}")

    authentication_access_tokens, refresh_token = s2.getAccessToken()
    access_token = s2.getAccessTokenViaRefreshToken(refresh_token)
    output_dir = os.getenv("outDir", "/app/data/")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # TODO check what to do here with downloaded data (where to store them locally)
    s2.download_files_concurrently(s2_results, output_dir, access_token)
