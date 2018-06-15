from django.conf.urls import url
import logging
logger = logging.getLogger(__name__)

from balsam.argo import views

local_urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^filter/$',views.filter,name='filter'),
    url(r'^job_display/(?P<job_num>\w+)/$',views.job_display,name='job_display'),
    ]

urlpatterns = local_urlpatterns 


