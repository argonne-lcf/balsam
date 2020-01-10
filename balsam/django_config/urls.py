from django.conf.urls import url,include
from balsam.django_config import views
from balsam.core import views as core_views
from balsam.core import urls as core_urls

urlpatterns = [
    url(r'^$', views.index, name="home"),
    url(r'^balsam/', include(core_urls)),
]
