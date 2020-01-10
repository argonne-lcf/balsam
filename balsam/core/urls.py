from django.conf.urls import url
from balsam.core import views

urlpatterns = [
    url(r'^add_app/$', views.add_app, name="add_app"),
    url(r'^add_job/$', views.add_job, name="add_job"),
    url(r'^edit_job/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/$', views.edit_job, name="edit_job"),
    url(r'^edit_app/(?P<app_id>\w+)/$', views.edit_app, name="edit_app"),
    url(r'^jobs/$', views.list_jobs, name="jobs"),
    url(r'^apps/$', views.list_apps, name="apps"),
    # url(r'^api/tasks_list', api_views.list_tasks, name="api_tasks"),
    # url(r'^api/tasks/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/$', 
    #     api_views.task_detail, name="api_task_detail"),
]
