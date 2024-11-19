class Message:
    """
    Defines the message.
    """
    _schema_def = {
        "namespace": "noa.harvester.order",
        "type": "object",
        "properties": {
            "succeded": {
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
    def schema() -> dict:
        """
        Returns the Schema definition of this type.
        """
        return Message._schema_def
