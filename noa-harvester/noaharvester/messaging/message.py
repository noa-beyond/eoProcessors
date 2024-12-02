class Message:
    """
    Defines the message.
    """

    _schema_request_def = {
        "namespace": "noa.harvester.order",
        "type": "object",
        "properties": {
            "uuid": {
                "type": "array",
                "items": {
                    "type": "string",
                    "uniqueItems": True,
                }
            }
        },
        "required": ["uuid"]
    }

    _schema_response_def = {
        "namespace": "noa.harvester.order",
        "type": "object",
        "properties": {
            "succeeded": {
                "type": "array",
                "items": {
                    "type": "string",
                    "uniqueItems": True,
                }
            },
            "failed": {
                "type": "array",
                "items": {
                    "type": "string",
                    "uniqueItems": True,
                }
            }
        },
        "required": ["succeeded", "failed"]
    }

    @staticmethod
    def schema_request() -> dict:
        """
        Returns the Schema definition of this type.
        """
        return Message._schema_request_def

    @staticmethod
    def schema_response() -> dict:
        """
        Returns the Schema definition of this type.
        """
        return Message._schema_response_def
