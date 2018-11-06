from django.conf.urls import url,include
from balsam.core import views as balsam_views
from balsam.core import urls as balsam_urls

urlpatterns = [
    url(r'^$', balsam_views.home_page, name="home"),
    url(r'^balsam/', include(balsam_urls)),
]
