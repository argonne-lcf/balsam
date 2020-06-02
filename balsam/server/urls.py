from django.urls import path, re_path, include
from rest_framework import permissions

from knox import views as knox_views
from drf_yasg.views import get_schema_view
from drf_yasg import openapi

from .views.main import (
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
    SessionList,
    SessionDetail,
)

schema_view = get_schema_view(
    openapi.Info(title="Balsam API", default_version="v1",),
    public=True,
    permission_classes=(permissions.AllowAny,),
)

urlpatterns = [
    # Schema
    re_path(
        r"swagger(?P<format>\.json|\.yaml)",
        schema_view.without_ui(cache_timeout=0),
        name="schema-json",
    ),
    path(
        r"swagger/",
        schema_view.with_ui("swagger", cache_timeout=0),
        name="schema-swagger-ui",
    ),
    path(r"redoc/", schema_view.with_ui("redoc", cache_timeout=0), name="schema-redoc"),
    # Auth & Users
    path(r"login", LoginView.as_view(), name="knox-login"),
    path(r"logout", knox_views.LogoutView.as_view(), name="knox-logout"),
    path(r"logoutall", knox_views.LogoutAllView.as_view(), name="knox-logoutall"),
    path(r"api-auth/", include("rest_framework.urls")),
    path("users/", UserList.as_view(), name="user-list"),
    path("users/<int:pk>", UserDetail.as_view(), name="user-detail"),
    # Sites & Apps
    path("sites/", SiteList.as_view(), name="site-list"),
    path("sites/<int:pk>", SiteDetail.as_view(), name="site-detail"),
    path("apps/", AppList.as_view(), name="app-list"),
    path("apps/<int:pk>", AppDetail.as_view(), name="app-detail"),
    path("apps/merge", AppMerge.as_view(), name="app-merge"),
    # Batch Jobs
    path("batchjobs/", BatchJobList.as_view(), name="batchjob-list"),
    path("batchjobs/<int:pk>", BatchJobDetail.as_view(), name="batchjob-detail"),
    path(
        "batchjobs/<int:batch_job_id>/jobs",
        JobList.as_view(),
        name="batchjob-ensemble-list",
    ),
    # Jobs, Associated EventLogs & Tranfsers
    path("jobs/", JobList.as_view(), name="job-list"),
    path("jobs/<int:pk>", JobDetail.as_view(), name="job-detail"),
    path("jobs/<int:job_id>/events", EventList.as_view(), name="job-event-list"),
    path("events/", EventList.as_view(), name="event-list"),
    # Job-processing Sessions
    path("sessions/", SessionList.as_view(), name="session-list"),
    path("sessions/<int:pk>", SessionDetail.as_view(), name="session-detail"),
]
