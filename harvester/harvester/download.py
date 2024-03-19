import os
import logging

import requests
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

log = logging.getLogger("harvester")


# TODO this class might have a wrong design: it is only for Copernicus
# It's fine, but a decision has to be made if we will have a generic one
# for all kind of "harvesting" sources in order to generalize it
class SatelliteSensorRequest:

    def __init__(
        self,
        username: str,
        password: str,
        start_date: str,
        end_date: str,
        bbox: str,
        tile: str | None = None,
        level: str | None = None,
    ) -> None:
        """
        Initialize the Download class.

        Parameters:
        start_date (str): The start date of the data to be downloaded.
        end_date (str): The end date of the data to be downloaded.
        bbox (str): The bounding box coordinates in the format "[min_lon, min_lat, max_lon, max_lat]".
        tile (str, optional): The tile identifier. Defaults to None.
        level (str, optional): Raster level product. 1, 2 or both (12)

        Raises:
        Exception: At least one of bbox or Tile must be provided
        """
        self.username = username
        self.password = password
        self.start_date = start_date
        self.end_date = end_date
        if bbox:
            self.bbox = parse_and_validate_bbox(bbox)
        elif tile is None:
            raise KeyError(
                "At least one of [bbox, tile] spatial parameter must be provided!"
            )
        else:
            self.bbox = None
            self.tile = tile

        self.level = level

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
            log.error(
                f"Failed to retrieve data from the server. Status code: {response.status_code} \n"
                f"Message: {response.reason}"
            )

    def getAccessTokenViaRefreshToken(self, refresh_token: str) -> str | None:

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
            return json_response.get("access_token", None)
        else:
            print(
                f"Failed to retrieve the access token. Status code: {response.status_code}"
            )


class Sentinel2Request(SatelliteSensorRequest):
    def __init__(
        self,
        username: str,
        password: str,
        start_date: str,
        end_date: str,
        bbox: str,
        tile: str,
        cloud_cover: int = 100,
        level: int = 2,
    ) -> None:

        super().__init__(username, password, start_date, end_date, bbox, tile, level)

        cloud_cover = int(cloud_cover)
        if cloud_cover < 0 or cloud_cover > 100:
            raise ValueError("Cloud coverage value must be between 0 and 100")
        self.cloud_cover = cloud_cover
        self.query = self.queryData()  # Fix: Added 'self.' before 'queryData()'

    # TODO is this the place where we construct the query, depending on the parameters?
    # If so, then the construction of the the url should be more elaborate
    def queryData(self) -> str:
        """
        Constructs and returns the query URL based on the provided parameters.

        Returns:
            str: The query URL.
        """
        if self.bbox:
            # box - a region of interest, defined as the rectangle with given (west, south, east, north) values.
            # It should be defined this way: &box=west,south,east,north
            # Like so, it is gets the values of minLongitude, minLatitude, maxLongitude, maxLatitude
            xmin, ymin, xmax, ymax = self.bbox
            return (
                f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?"
                f"startDate={self.start_date}T00:00:00Z&"
                f"completionDate={self.end_date}T23:59:59Z&"
                f"maxRecords=1000&"
                f"box={xmin},{ymin},{xmax},{ymax}&"
                f"cloudCover=[0,{self.cloud_cover}]"
            )
        else:
            return (
                f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?"
                f"startDate={self.start_date}T00:00:00Z&"
                f"completionDate={self.end_date}T23:59:59Z&"
                f"maxRecords=1000&"
                f"cloudCover=[0,{self.cloud_cover}]"
            )

    def search(self) -> dict:
        """
        Searches for data based on the provided query.

        Returns:
            dict: A dictionary containing the search results, where the keys are identifiers and the values are lists
                  containing the selected tile, begin date, cloud coverage, and quicklook URL.
        """
        session = requests.Session()
        session.stream = True

        response = session.get(self.query)
        if response.status_code != 200:
            log.error(
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
                if self.level == "1" and "L2A" in identifier.split("_")[1]:
                    continue
                # TODO WHat's the meaning of the following? Only one caracter is selected
                # if "L2A" in identifier.split("_")[1] and level == "1":
                #     continue
                # TODO: why do you remove the T here?
                # selected_tile = feature["properties"]["title"].split("_")[5][1:]
                selected_tile = feature["properties"]["title"].split("_")[5]
                begin_date = feature["properties"]["startDate"]
                cloud_coverage = feature["properties"]["cloudCover"]
                download_url = feature["properties"]["services"]["download"]["url"]
                id = feature["id"]
                results[identifier] = [
                    selected_tile,
                    begin_date,
                    cloud_coverage,
                    quicklook,
                    download_url,
                    id,
                ]
                # TODO: Should we handle this exception, even log-wise?
            except Exception as e:
                log.error(f"Error while parsing server response : {e}")
                continue
        return results

    def download(self, results: dict) -> None:
        # showResultsAsTable(results)
        log.info("Searching results:")
        time.sleep(10)
        log.info("Downloading results:")

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

            log.info(f"Downloaded {filename}")

    def download_file(self, session: requests.Session, url: str, filename: str) -> None:
        with session.get(url, stream=True) as response:
            response.raise_for_status()  # This will raise an error for non-200 response
            total_size_in_bytes = int(response.headers.get("content-length", 0))
            block_size = 8192  # 8 Kilobytes

            progress_bar = tqdm(
                total=total_size_in_bytes,
                unit="iB",
                unit_scale=True,
                desc=filename,
                leave=True,
            )
            with open(filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=block_size):
                    progress_bar.update(len(chunk))
                    file.write(chunk)
            progress_bar.close()

            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                log.error(f"ERROR, something went wrong downloading {filename}")

    def download_files_concurrently(
        self, results: dict, output_dir: str, access_token: str
    ) -> None:
        headers = {"Authorization": f"Bearer {access_token}"}
        session = requests.Session()
        session.headers.update(headers)

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            # TODO: here, only the values[-1] is used from results. Why do we need the rest of results?
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
def parse_and_validate_bbox(bbox: str) -> list:
    """
    Checks if the bounding box coordinates(string) are valid and returns the bbox list of these coordinates

    Returns:
        list: The bbox coordinates (west, south, east, north) as a List
    Raises:
        AttributeException: if bbox is not a comma separated string or 4 coordinates are not correct.
    """

    message = (
        "Spatial Parameters are non-correct: "
        "Bounding box coordinates are not valid."
        "[min_lon, min_lat, max_lon, max_lat] are required."
    )

    try:
        bbox_list = [float(x) for x in bbox.split(",")]
        if len(bbox_list) == 4:
            if bbox_list[0] < bbox_list[2] and bbox_list[1] < bbox_list[3]:
                return bbox_list
    except Exception:
        raise AttributeError(message)


# TODO test that and notify of missing parameters
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
    if "start_date" in os.environ:
        parameters["start_date"] = os.environ["start_date"]
    if "end_date" in os.environ:
        parameters["end_date"] = os.environ["end_date"]
    if "bbox" in os.environ:
        parameters["bbox"] = os.environ["bbox"]
    else:
        parameters["bbox"] = None
    if "tile" in os.environ:
        parameters["tile"] = os.environ["tile"]
    else:
        parameters["tile"] = None
    if "cloud_cover" in os.environ:
        parameters["cloud_cover"] = os.environ["cloud_cover"]
    if "level" in os.environ:
        parameters["level"] = os.environ["level"]
    return parameters


def showResultsAsTable(results: dict):
    """
    Show the results as a table.

    Parameters:
    results (dict): The results.
    """
    print("Tile\tBegin Date\tCloud Coverage\tDownload URL")
    for identifier, values in results.items():
        print(f"{values[0]}\t{values[1]}\t{values[2]}\t{values[-1]}")


def checkParameters(satellite: str, parameters: dict):
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
            and "start_date" in parameters
            and "end_date" in parameters
            and ("bbox" in parameters or "tile" in parameters)
            and "cloud_cover" in parameters
        ):
            return True
    return False


# TODO: FIX This "main", checks for S2 only. And the parameters are satellite specific (like level)
# TODO: main should only call constructors or start a "pipeline"
if __name__ == "__main__":
    if not checkParameters("Sentinel2", getParametersFromDockerEnv()):
        SystemError("Incorrect or/and insufficient parameters")

    username, password, start_date, end_date, bbox, tile, cloud_cover, level = (
        getParametersFromDockerEnv().values()
    )

    if str(level) not in ["1", "2", "12"]:
        raise ValueError("Incorrect level parameter. It must be 1, 2 or 12")

    # TODO introduce the level. It was introduced but the constructor cannot receive it
    s2 = Sentinel2Request(
        username, password, start_date, end_date, bbox, tile, cloud_cover, level
    )
    s2_results = s2.search()
    print(f"# of products found:{len(s2_results)}")
    # the number of results (not sure if this is the correct way)
    # also defines the number of pages which will return
    # check here https://documentation.dataspace.copernicus.eu/APIs/OpenSearch.html
    # actually, you might want to return pages of 20 results each and navigate
    authentication_access_tokens, refresh_token = s2.getAccessToken()
    access_token = s2.getAccessTokenViaRefreshToken(refresh_token)
    output_dir = os.getenv("outDir", "/app/data/")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # TODO check what to do here with downloaded data (where to store them locally)
    s2.download_files_concurrently(s2_results, output_dir, access_token)
