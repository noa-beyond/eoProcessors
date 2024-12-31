from rest_framework import serializers

class CollectionSerializer(serializers.Serializer):
    id = serializers.CharField()
    title = serializers.CharField()
    description = serializers.CharField()

class ItemSerializer(serializers.Serializer):
    id = serializers.CharField()
    geometry = serializers.JSONField()
    properties = serializers.JSONField()