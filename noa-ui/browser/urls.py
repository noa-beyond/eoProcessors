from django.urls import path
from . import views

urlpatterns = [
    path("", views.results, name="results"),
    # path("search/", views.search, name="search"),
    path('results/', views.results, name='results'),  # Results view
    path('submit-order/', views.submit_order, name='submit_order'), 
    path('dashboard/', views.user_dashboard, name='user_dashboard'),
]
