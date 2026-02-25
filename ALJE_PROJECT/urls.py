"""
URL configuration for ALJE_PROJECT project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path,include, re_path
from ALJE_APP.views import *

urlpatterns = [
    path('admin/', admin.site.urls),
    path('Outbound/',include("ALJE_APP.urls")),
    path('NewOutbound/', include('New_Outbound_App.urls')),
    
    path('Location_Mapping/', include('Location_Map_App.urls')),
    path("Inbound/", include("Inbound_App.urls") ),
]
