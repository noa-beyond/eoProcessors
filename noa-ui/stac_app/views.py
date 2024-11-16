from django.http import JsonResponse
from rest_framework.decorators import api_view
from django.db import connection

@api_view(['GET'])
def get_collections(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT id, title, description FROM collections")
        rows = cursor.fetchall()
        collections = [{'id': row[0], 'title': row[1], 'description': row[2]} for row in rows]
    return JsonResponse(collections, safe=False)

@api_view(['GET'])
def get_items(request, collection_id):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT id, geometry, properties
            FROM items
            WHERE collection_id = %s
        """, [collection_id])
        rows = cursor.fetchall()
        items = [{'id': row[0], 'geometry': row[1], 'properties': row[2]} for row in rows]
    return JsonResponse(items, safe=False)
