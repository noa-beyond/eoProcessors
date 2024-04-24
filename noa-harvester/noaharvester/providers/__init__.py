from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class DataProvider(ABC):
    """
    Abstract Data Provider.

    Methods:
        query: Query for specific search terms.
        download: Download items based on search terms.
        describe(optional): Describe collection.
    """

    def __init__(self) -> DataProvider:
        """
        Instantiates a new Data Provider class.

        Attributes:
            _download_path: Data are stored under /data of local execution folder.
        """
        self._download_path = Path("./data").absolute()

    # @property
    # def config(self):
    #     return self._config

    @abstractmethod
    def query(self):
        """
        Query data provider for collection items.
        """

    @abstractmethod
    def download(self):
        """
        Download data for collection items.
        """

    @abstractmethod
    def describe(self):
        """
        Describe available collection query parameters.
        """
