from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),  # Default view for rendering the UI
    path('collections/', views.get_collections, name='get_collections'),
    # path('collections/<str:collection_id>/items/', views.get_items, name='get_items'),
    path('items_page/', views.items_page, name='items_page'),

]