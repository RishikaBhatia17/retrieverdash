from django.urls import path
from .views import DashboardView, DiffView, OldView

app_name = 'core'
urlpatterns = [
    path('diff/<str:filename>/', DiffView.as_view(), name='diff'),
    path('old/<str:filename>/', OldView.as_view(), name='old'),
    path('', DashboardView.as_view(), name='dashboard'),
]
