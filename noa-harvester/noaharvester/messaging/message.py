class Message:
    """
    Defines the message.
    """
    _schema_def = {
        "namespace": "harvester.message.types",
        "type": "object",
        "properties": {
            "transaction-id": {"type": "string"},
            "payload": {
                "type": "object",
                "properties": {
                    "message": {"type": "string", "const": "undefined"}
                },
                "required": ["message"]
            }
        },
        "required": ["transaction-id", "payload"]
    }

    @staticmethod
    def schema() -> dict:
        """
        Returns the Schema definition of this type.
        """
        return Message._schema_def
