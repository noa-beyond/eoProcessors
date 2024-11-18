class AbstractProducer(object):
    """
    Abstract Kafka Producer doing nothing.
    """
    def __init__(self, bootstrap_servers: list, schema: dict) -> None:
        """
        Create the Producer instance.
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema = schema

    def send(self, topic: str, key: str, value: dict) -> None:
        """
        Send the specified Value to a Kafka Topic.
        """
