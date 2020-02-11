from django.urls import path, include
from rest_framework.schemas import get_schema_view

from knox import views as knox_views
from .views.main import (
    api_root,
    LoginView,
    UserList,
    UserDetail,
    SiteList,
    SiteDetail,
    AppList,
    AppDetail,
    AppMerge,
    BatchJobList,
    BatchJobDetail,
    JobList,
    JobDetail,
    EventList,
)

schema_view = get_schema_view(title="Balsam API")

urlpatterns = [
    path("", api_root, name="api-root"),
    path("schema", schema_view),
    path(r"login", LoginView.as_view(), name="knox-login"),
    path(r"logout", knox_views.LogoutView.as_view(), name="knox-logout"),
    path(r"logoutall", knox_views.LogoutAllView.as_view(), name="knox-logoutall"),
    path(r"api-auth/", include("rest_framework.urls")),
    path("users/", UserList.as_view(), name="user-list"),
    path("users/<int:pk>", UserDetail.as_view(), name="user-detail"),
    path("sites/", SiteList.as_view(), name="site-list"),
    path("sites/<int:pk>", SiteDetail.as_view(), name="site-detail"),
    path("apps/", AppList.as_view(), name="app-list"),
    path("apps/<int:pk>", AppDetail.as_view(), name="app-detail"),
    path("apps/merge", AppMerge.as_view(), name="app-merge"),
    path("batchjobs/", BatchJobList.as_view(), name="batchjob-list"),
    path("batchjobs/<int:pk>", BatchJobDetail.as_view(), name="batchjob-detail"),
    path(
        "batchjobs/<int:batch_job_id>/jobs",
        JobList.as_view(),
        name="batchjob-ensemble-list",
    ),
    path("jobs/", JobList.as_view(), name="job-list"),
    path("jobs/<int:pk>", JobDetail.as_view(), name="job-detail"),
    path("jobs/<int:job_id>/events", EventList.as_view(), name="job-event-list"),
]
