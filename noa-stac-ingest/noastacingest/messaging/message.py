class Message:
    """
    Defines the message.
    """

    _schema_request_chdm_def = {
        "namespace": "noa.stacingest.request",
        "type": "object",
        "properties": {
            "orderId": {"type": "string"},
            "noaId": {"type": "string"},
            "noaS3Path": {"type": "string"},
        },
        "required": ["orderId", "noaId", "noaS3Path"],
    }

    _schema_request_def = {
        "namespace": "noa.stacingest.order",
        "type": "object",
        "properties": {
            "Ids": {
                "type": "array",
                "items": {
                    "type": "string",
                    "uniqueItems": True,
                },
            }
        },
        "required": ["Ids"],
    }

    _schema_response_def = {
        "namespace": "noa.stacingest.response",
        "type": "object",
        "properties": {
            "succeeded": {
                "type": "array",
                "items": {
                    "type": "string",
                    "uniqueItems": True,
                },
            },
            "failed": {
                "type": "array",
                "items": {
                    "type": "string",
                    "uniqueItems": True,
                },
            },
        },
        "required": ["succeeded", "failed"],
    }

    _schema_response_chdm_def = {
        "namespace": "noa.stacingest.response",
        "type": "object",
        "properties": {"orderId": {"type": "string"}, "result": {"type": "number"}},
        "required": ["orderId", "result"],
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

    @staticmethod
    def schema_response_chdm() -> dict:
        """
        Returns the Schema definition of this type.
        """
        return Message._schema_response_chdm_def
