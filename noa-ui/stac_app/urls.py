from django.urls import path
from stac_app import views

urlpatterns = [
    path('collections/', views.get_collections, name='get_collections'),
    path('collections/<str:collection_id>/items/', views.get_items, name='get_items'),
]