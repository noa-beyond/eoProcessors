import requests
from datetime import datetime

from django.shortcuts import render
from django.http import JsonResponse

from utils.geo_utils import bbox_to_polygon

API_BASE_URL = "https://localhost:7236/api" 


def map_view(request):
    return render(request, 'base.html')

def search(request):
    if request.method == "POST":
        data_source = request.POST.get("data_source")
        start_date = request.POST.get("start_date") + "T00:00:00.000"
        end_date = request.POST.get("end_date", datetime.now().strftime("%Y-%m-%d"))
        geometry = request.POST.get("bbox")
        cloud_coverage = request.POST.get("cloud_coverage", None)
        product_type = request.POST.get("product_type", None)
        relative_orbit = request.POST.get("relative_orbit", None)

        geometry = [coordinate for coordinate in geometry.split(",")]

        if data_source == "Sentinel-2":
            payload = {
                "provider": 2, 
                "startDate": start_date,
                "geometry": bbox_to_polygon(geometry),
                "properties": {
                    "cloudCoverage": cloud_coverage
                },
            }
            endpoint = f"{API_BASE_URL}/SatelliteProduct"
        
        elif data_source == "Sentinel-1":
            payload = {
                "provider": 1,  
                "startDate": start_date,
                "geometry": bbox_to_polygon(geometry),
                "properties": {
                    "productType": product_type,
                    "relativeOrbit": relative_orbit,
                },
            }
            endpoint = f"{API_BASE_URL}/SatelliteProduct"
        
        else:
            return JsonResponse({"error": "Invalid data source"}, status=400)

        try:
            response = requests.post(endpoint, json=payload, verify=False) 
            response.raise_for_status()
        
        except requests.exceptions.RequestException as e:
            return JsonResponse({"error": str(e)}, status=500)

        return JsonResponse(response.json())

    return render(request, "search.html")
