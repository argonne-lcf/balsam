from django.contrib.auth import get_user_model
from django.shortcuts import get_object_or_404, redirect
from django.http import Http404

from rest_framework import generics, permissions, views
from rest_framework.pagination import LimitOffsetPagination
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework.reverse import reverse
from rest_framework import filters as drf_filters
from rest_framework.authentication import BasicAuthentication
from rest_framework import status

import django_filters.rest_framework as django_filters
from knox.views import LoginView as KnoxLoginView
from balsam.server import serializers as ser
from .bulk import (
    ListSingleCreateBulkUpdateAPIView, ListBulkCreateBulkUpdateBulkDestroyAPIView
)
from balsam.server.models import Site, AppExchange, Job, BatchJob, EventLog

User = get_user_model()

@api_view(['GET'])
def api_root(request):
    return redirect(
        reverse('user-detail', kwargs={"pk":request.user.pk}, request=request)
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
    limit_query_param = 'limit'
    offset_query_param = 'offset'

class JSONFilter(drf_filters.BaseFilterBackend):
    """
    View must define `json_filter_field` and `json_filter_type`
    Passes through any supported JSON lookup:
        tags__has_key="foo"
        tags__foo__icontains="x"
        tags__foo="x"
    All query params are strings unless the view sets
    `json_filter_value_type` to a callable like `json.loads`
    """
    def filter_queryset(self, request, queryset, view):
        param = view.json_filter_field + '__'
        decoder = getattr(view, 'json_filter_type', str)
        tags = {
            k:decoder(v) for k,v in request.query_params.items()
            if k.startswith(param)
        }
        return queryset.filter(**tags)

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
        return user.apps.all().select_related('owner').prefetch_related('users', 'backends')
    
    def perform_destroy(self, instance):
        instance.delete()

class AppMerge(generics.CreateAPIView):
    queryset = AppExchange.objects.all()
    serializer_class = ser.AppMergeSerializer
    permission_classes = [permissions.IsAuthenticated]

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)
    
class BatchJobFilter(django_filters.FilterSet):
    start_time = django_filters.IsoDateTimeFromToRangeFilter()
    end_time = django_filters.IsoDateTimeFromToRangeFilter()
    class Meta:
        model = BatchJob
        fields = [
            'site', 'scheduler_id', 'project', 'queue', 'num_nodes', 
            'wall_time_min', 'job_mode', 'start_time', 'end_time', 
            'state', 'scheduler_id'
        ]

class BatchJobList(ListSingleCreateBulkUpdateAPIView):
    serializer_class = ser.BatchJobSerializer
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = BalsamPaginator
    filter_backends = [
        django_filters.DjangoFilterBackend,
        drf_filters.SearchFilter,
        drf_filters.OrderingFilter,
        JSONFilter
    ]
    filterset_class = BatchJobFilter
    json_filter_field = "filter_tags"
    json_filter_type = str
    ordering_fields = ['start_time', 'end_time', 'state'] # ?ordering=-end_time,state
    search_fields = ['site__hostname', 'site__path'] # partial matching across fields
    
    def get_queryset(self):
        user = self.request.user
        return BatchJob.objects.filter(site__owner=user).order_by('-start_time')

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

    def get_queryset(self):
        qs = self.request.user.jobs.all()
        batch_job_id = self.kwargs.get('batch_job_id')
        if batch_job_id is not None:
            qs = qs.filter(batch_job=batch_job_id)
        qs = qs.select_related('site', 'owner', 'app_exchange', 'app_backend')
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

    def get_queryset(self):
        qs = EventLog.objects.filter(job__owner=self.request.user)
        job_id = self.kwargs.get('job_id')
        if job_id is not None:
            qs = qs.filter(job=job_id)
        return qs