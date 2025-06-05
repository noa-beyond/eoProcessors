class Message:
    """
    Defines the message.
    """

    _schema_request_def = {
        "namespace": "noa.chdm.request",
        "type": "object",
        "properties": {
            "orderId": {
                "type": "string"
            },
            "initialSelectionProductPaths": {
                "type": "array",
                "items": {
                    "type": "string",
                    "uniqueItems": True,
                }
            },
            "finalSelectionProductPaths": {
                "type": "array",
                "items": {
                    "type": "string",
                    "uniqueItems": True,
                }
            },
            "bbox": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            }
        },
        "required": [
            "orderId",
            "initialSelectionProductPaths",
            "finalSelectionProductPaths",
            "bbox"
        ]
    }

    # NOTE product path which includes the two sub-products: binary and confidence
    _schema_response_def = {
        "namespace": "noa.chdm.response",
        "type": "object",
        "properties": {
            "result": {
                "type": "string",
            },
            "orderId": {
                "type": "string",
            },
            "chdmProductPath": {
                "type": "string",
            }
        },
        "required": [
            "result",
            "orderId",
            "chdmProductPath"
        ]
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
