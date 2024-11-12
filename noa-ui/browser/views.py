from django.shortcuts import render

from django.shortcuts import render
from django.http import JsonResponse

def map_view(request):
    return render(request, 'base.html')

def search(request):
    if request.method == 'POST':
        data_source = request.POST.get('data_source')
        cloud_coverage = request.POST.get('cloud_coverage')
        start_date = request.POST.get('start_date')
        end_date = request.POST.get('end_date')
        geometry = request.POST.get('geometry')
        # Process the search parameters (e.g., call an external API)
        response_data = {
            "data_source": data_source,
            "cloud_coverage": cloud_coverage,
            "start_date": start_date,
            "end_date": end_date,
            "geometry": geometry,
        }
        return JsonResponse(response_data)
