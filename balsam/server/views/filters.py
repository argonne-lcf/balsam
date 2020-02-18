from rest_framework import filters as drf_filters
from rest_framework.filters import SearchFilter, OrderingFilter  # noqa
import django_filters.rest_framework as django_filters
from django_filters.rest_framework import DjangoFilterBackend  # noqa
from balsam.server.models import BatchJob, Job, EventLog


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

    class Meta:
        model = EventLog
        fields = [
            "from_state",
            "to_state",
            "timestamp",
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
    site_id = django_filters.NumberFilter(
        field_name="app_backend", lookup_expr="site_id"
    )
    site_hostname = django_filters.CharFilter(
        field_name="app_backend", lookup_expr="site__hostname"
    )
    site_path = django_filters.CharFilter(
        field_name="app_backend", lookup_expr="site__path"
    )
    batch_job_id = django_filters.NumberFilter(
        field_name="batch_job", lookup_expr="scheduler_id"
    )
    last_update = django_filters.IsoDateTimeFromToRangeFilter()
    parents = django_filters.ModelMultipleChoiceFilter(
        field_name="parents", queryset=jobs_qs
    )

    class Meta:
        model = Job
        fields = [
            "pk",
            "workdir",
            "app_id",
            "app_name",
            "app_class",
            "site_id",
            "site_hostname",
            "site_path",
            "state",
            "batch_job_id",
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
