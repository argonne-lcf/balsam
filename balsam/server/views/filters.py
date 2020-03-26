from rest_framework import filters as drf_filters
from rest_framework.filters import SearchFilter, OrderingFilter  # noqa
import django_filters.rest_framework as django_filters
from django_filters.rest_framework import DjangoFilterBackend  # noqa
from balsam.server.models import BatchJob, Job, EventLog, Site, AppExchange

# NOTE: AllValuesMultipleFilter, MultipleChoiceFilter, etc.. take
# query parameters that are AND'd together ?foo=A&foo=B.
# BaseInFilter subclasses take a single query parameter with comma-sep values


class JSONFilter(drf_filters.BaseFilterBackend):
    """
    View must define `json_filter_fields`
    Passes through any supported JSON lookup:
        tags__has_key="foo"
        tags__foo__icontains="x"
        tags__foo="x"
    Only supports querying on string values
    """

    def filter_queryset(self, request, queryset, view):
        fields = getattr(view, "json_filter_fields", [])
        kwargs = {}
        for field in fields:
            kwargs.update(
                {
                    param: value
                    for param, value in request.query_params.items()
                    if param.startswith(field + "__")
                }
            )
        return queryset.filter(**kwargs) if kwargs else queryset


class CharInFilter(django_filters.BaseInFilter, django_filters.CharFilter):
    pass


class SiteFilter(django_filters.FilterSet):
    pk = django_filters.AllValuesMultipleFilter(field_name="pk")
    path__contains = django_filters.CharFilter(
        field_name="path", lookup_expr="icontains"
    )

    class Meta:
        model = Site
        fields = ["pk", "hostname", "path", "path__contains"]


class AppFilter(django_filters.FilterSet):
    pk = django_filters.AllValuesMultipleFilter(field_name="pk")
    owner = django_filters.CharFilter(field_name="owner", lookup_expr="username")
    site = django_filters.NumberFilter(field_name="backends", lookup_expr="site_id")
    site_hostname = django_filters.CharFilter(
        field_name="backends", lookup_expr="site__hostname"
    )
    site_path = django_filters.CharFilter(
        field_name="backends", lookup_expr="site__path"
    )
    class_name = django_filters.CharFilter(
        field_name="backends", lookup_expr="class_name"
    )
    name__in = CharInFilter(field_name="name", lookup_expr="in")

    class Meta:
        model = AppExchange
        fields = [
            "pk",
            "name",
            "name__in",
            "owner",
            "site",
            "site_hostname",
            "site_path",
            "class_name",
        ]


class BatchJobFilter(django_filters.FilterSet):
    start_time = django_filters.IsoDateTimeFromToRangeFilter()
    end_time = django_filters.IsoDateTimeFromToRangeFilter()

    class Meta:
        model = BatchJob
        fields = [
            "site",
            "scheduler_id",
            "project",
            "queue",
            "num_nodes",
            "wall_time_min",
            "job_mode",
            "start_time",
            "end_time",
            "state",
            "scheduler_id",
        ]


class EventFilter(django_filters.FilterSet):
    timestamp = django_filters.IsoDateTimeFromToRangeFilter()
    batch_job_id = django_filters.NumberFilter(
        field_name="job", lookup_expr="batch_job_id"
    )
    job_id = django_filters.NumberFilter(field_name="job_id")
    scheduler_id = django_filters.NumberFilter(
        field_name="job", lookup_expr="batch_job__scheduler_id"
    )
    message__contains = django_filters.CharFilter(
        field_name="message", lookup_expr="icontains"
    )

    class Meta:
        model = EventLog
        fields = [
            "from_state",
            "to_state",
            "timestamp",
            "batch_job_id",
            "job_id",
            "scheduler_id",
            "message__contains",
        ]


def jobs_qs(request):
    if request is not None:
        return Job.objects.filter(owner=request.user)
    return Job.objects.none()


class JobFilter(django_filters.FilterSet):
    pk = django_filters.AllValuesMultipleFilter(field_name="pk")
    app_id = django_filters.NumberFilter(field_name="app_exchange", lookup_expr="pk")
    app_name = django_filters.CharFilter(field_name="app_exchange", lookup_expr="name")
    app_class = django_filters.CharFilter(
        field_name="app_backend", lookup_expr="class_name"
    )
    workdir__contains = django_filters.CharFilter(
        field_name="workdir", lookup_expr="contains"
    )
    site_id = django_filters.NumberFilter(
        field_name="app_backend", lookup_expr="site_id"
    )
    site_hostname = django_filters.CharFilter(
        field_name="app_backend", lookup_expr="site__hostname"
    )
    site_path = django_filters.CharFilter(
        field_name="app_backend", lookup_expr="site__path"
    )
    batch_job_id = django_filters.NumberFilter(field_name="batch_job_id")
    scheduler_id = django_filters.NumberFilter(
        field_name="batch_job", lookup_expr="scheduler_id"
    )
    last_update = django_filters.IsoDateTimeFromToRangeFilter()
    parents = django_filters.ModelMultipleChoiceFilter(
        field_name="parents", queryset=jobs_qs
    )
    state__ne = django_filters.CharFilter(field_name="state", exclude=True)

    class Meta:
        model = Job
        fields = [
            "pk",
            "workdir",
            "workdir__contains",
            "app_id",
            "app_name",
            "app_class",
            "site_id",
            "site_hostname",
            "site_path",
            "state",
            "state__ne",
            "batch_job_id",  # the unique PK stored in BalsamDB
            "scheduler_id",  # the job's local Scheduler ID
            "last_update",
            "last_error",
            "parents",
            "num_nodes",
            "ranks_per_node",
            "threads_per_rank",
            "threads_per_core",
            "cpu_affinity",
            "gpus_per_rank",
            "node_packing_count",
            "wall_time_min",
        ]
