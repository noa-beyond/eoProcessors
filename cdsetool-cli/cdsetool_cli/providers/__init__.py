from abc import ABC, abstractmethod
from pathlib import Path


class DataProvider(ABC):
    """
        Abstract class to differentiate query and download functions for each provider
    """
    def __init__(self):
        self._download_path = Path("./data").absolute()

    # @property
    # def config(self):
    #     return self._config

    @abstractmethod
    def query(self):
        """
            Abstract method to query data provider for collection items
        """
        pass

    @abstractmethod
    def download(self):
        pass

    @abstractmethod
    def describe(self):
        pass
