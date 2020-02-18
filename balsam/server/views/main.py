from django.contrib.auth import get_user_model
from django.shortcuts import redirect

from rest_framework import generics, permissions
from rest_framework.pagination import LimitOffsetPagination
from rest_framework.decorators import api_view
from rest_framework.reverse import reverse
from rest_framework.response import Response
from rest_framework import status
from rest_framework.authentication import BasicAuthentication

from knox.views import LoginView as KnoxLoginView
from balsam.server import serializers as ser
from .bulk import (
    ListSingleCreateBulkUpdateAPIView,
    ListBulkCreateBulkUpdateBulkDestroyAPIView,
)
from .filters import (
    JSONFilter,
    BatchJobFilter,
    JobFilter,
    EventFilter,
    DjangoFilterBackend,
    SearchFilter,
    OrderingFilter,
)
from balsam.server.models import Site, AppExchange, Job, BatchJob, EventLog, JobLock

User = get_user_model()


@api_view(["GET"])
def api_root(request):
    return redirect(
        reverse("user-detail", kwargs={"pk": request.user.pk}, request=request)
    )


class IsAuthenticatedOrAdmin(permissions.BasePermission):
    """
    Admin sees all Users; User sees self only
    """

    def has_permission(self, request, view):
        if request.user.is_staff:
            return True
        return view.kwargs["pk"] == request.user.pk


class IsOwner(permissions.BasePermission):
    def has_object_permission(self, request, view, obj):
        return obj.owner == request.user


class IsOwnerOrReadOnly(permissions.BasePermission):
    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True
        return obj.owner == request.user


class BalsamPaginator(LimitOffsetPagination):
    default_limit = 100
    max_limit = 5000
    limit_query_param = "limit"
    offset_query_param = "offset"


class LoginView(KnoxLoginView):
    authentication_classes = [BasicAuthentication]


class UserList(generics.ListAPIView):
    queryset = User.objects.all()
    serializer_class = ser.UserSerializer
    permission_classes = [permissions.IsAdminUser]


class UserDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = User.objects.all()
    serializer_class = ser.UserSerializer
    permission_classes = [IsAuthenticatedOrAdmin]


class SiteList(generics.ListCreateAPIView):
    queryset = Site.objects.all()
    serializer_class = ser.SiteSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return user.sites.all()

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)


class SiteDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = Site.objects.all()
    serializer_class = ser.SiteSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return user.sites.all()

    def perform_destroy(self, instance):
        instance.delete()


class AppList(generics.ListCreateAPIView):
    queryset = AppExchange.objects.all()
    serializer_class = ser.AppSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return user.apps.all()

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)


class AppDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = AppExchange.objects.all()
    serializer_class = ser.AppSerializer
    permission_classes = [permissions.IsAuthenticated, IsOwnerOrReadOnly]

    def get_queryset(self):
        user = self.request.user
        return (
            user.apps.all()
            .select_related("owner")
            .prefetch_related("users", "backends")
        )

    def perform_destroy(self, instance):
        instance.delete()


class AppMerge(generics.CreateAPIView):
    queryset = AppExchange.objects.all()
    serializer_class = ser.AppMergeSerializer
    permission_classes = [permissions.IsAuthenticated]

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)


class BatchJobList(ListSingleCreateBulkUpdateAPIView):
    serializer_class = ser.BatchJobSerializer
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = BalsamPaginator
    filter_backends = [
        DjangoFilterBackend,
        SearchFilter,
        OrderingFilter,
        JSONFilter,
    ]
    filterset_class = BatchJobFilter
    json_filter_fields = ["filter_tags"]
    ordering_fields = ["start_time", "end_time", "state"]  # ?ordering=-end_time,state
    search_fields = ["site__hostname", "site__path"]  # partial matching across fields

    def get_queryset(self):
        user = self.request.user
        return BatchJob.objects.filter(site__owner=user).order_by("-start_time")


class BatchJobDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = BatchJob.objects.all()
    serializer_class = ser.BatchJobSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return BatchJob.objects.filter(site__owner=user)


class JobList(ListBulkCreateBulkUpdateBulkDestroyAPIView):
    queryset = Job.objects.all()
    serializer_class = ser.JobSerializer
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = BalsamPaginator
    filter_backends = [
        DjangoFilterBackend,
        OrderingFilter,
        JSONFilter,
    ]
    filterset_class = JobFilter
    json_filter_fields = ["tags", "parameters", "data"]
    ordering_fields = [
        "last_update",
        "pk",
        "workdir",
        "state",
    ]

    def get_queryset(self):
        qs = self.request.user.jobs.all()
        batch_job_id = self.kwargs.get("batch_job_id")
        if batch_job_id is not None:
            qs = qs.filter(batch_job=batch_job_id)
        qs = qs.select_related("site", "owner", "app_exchange", "app_backend")
        return qs

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)

    def perform_destroy(self, queryset):
        Job.objects.bulk_delete_queryset(queryset)


class JobDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = Job.objects.all()
    serializer_class = ser.JobSerializer
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = BalsamPaginator

    def get_queryset(self):
        qs = self.request.user.jobs.all()
        return qs


class EventList(generics.ListAPIView):
    queryset = EventLog.objects.all()
    serializer_class = ser.EventLogSerializer
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = BalsamPaginator
    filter_backends = [
        DjangoFilterBackend,
        OrderingFilter,
        SearchFilter,
        JSONFilter,
    ]
    filterset_class = EventFilter
    json_filter_fields = ["job__tags"]
    ordering_fields = ["timestamp"]
    search_fields = ["message"]

    def get_queryset(self):
        qs = EventLog.objects.filter(job__owner=self.request.user)
        job_id = self.kwargs.get("job_id")
        if job_id is not None:
            qs = qs.filter(job=job_id).order_by("timestamp")
        return qs


class SessionList(generics.ListCreateAPIView):
    queryset = JobLock.objects.all()
    serializer_class = ser.SessionSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return JobLock.objects.filter(site__owner=self.request.user)


class SessionDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = JobLock.objects.all()
    serializer_class = ser.SessionSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return JobLock.objects.filter(site__owner=self.request.user)

    def post(self, request, *args, **kwargs):
        """acquire()"""
        lock_instance = self.get_object()
        serializer = ser.JobAcquireSerializer(
            data=request.data, many=False, context={"request": request}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save(lock=lock_instance)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def perform_destroy(self, job_lock_instance):
        job_lock_instance.release()
