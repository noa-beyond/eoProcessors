from django.http import JsonResponse
from rest_framework.decorators import api_view
from django.db import connection
from django.shortcuts import render

@api_view(['GET'])
def get_collections(request):
    """
    Fetches all collections from the pgstac.collections table.
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT id, content->>'title' AS title, content->>'description' AS description
            FROM pgstac.collections
        """)
        rows = cursor.fetchall()
        collections = [{'id': row[0], 'title': row[1], 'description': row[2]} for row in rows]
    return JsonResponse(collections, safe=False)

from django.http import JsonResponse
from django.db import connection
import json

def items_page(request, collection_id=None):
    """
    Fetch items for a given collection_id and render them in a template.
    """
    query = """
        SELECT id, ST_AsGeoJSON(ST_Transform(geometry, 4326)), content
        FROM pgstac.items
    """
    params = []
    if collection_id:
        query += " WHERE collection = %s"
        params.append(collection_id)

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        items = []
        for row in rows:
            item_id = row[0]
            geometry = json.loads(row[1])  # Parse GeoJSON string into a dictionary
            content = json.loads(row[2])  # Parse content JSON into a dictionary
            
            # Merge geometry into the content
            content['geometry'] = geometry
            
            # Append the result
            items.append({
                'id': item_id,
                'geometry': geometry,
                'assets': content.get('assets', {}),
            })

    # Pass the items to the template
    return render(request, 'stac_app/index.html', {'items_json': json.dumps(items)})

def index(request):
    """
    Renders the index.html template for the STAC application.
    """
    return render(request, 'stac_app/index.html')
