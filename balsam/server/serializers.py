from pathlib import Path
from rest_framework import serializers
from balsam.server.models import (
    User,
    AppExchange,
    AppBackend,
    Site,
    SiteStatus,
    BatchJob,
    Job,
    JobLock,
    TransferItem,
    EventLog,
    JOB_STATE_CHOICES,
)
from .bulk_serializer import CachedPrimaryKeyRelatedField, BulkModelSerializer

ValidationError = serializers.ValidationError

# OWNERSHIP-AWARE FIELDS
# -----------------------
class OwnedSitePrimaryKeyRelatedField(CachedPrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context["request"].user
        return Site.objects.filter(owner=user)


class OwnedAppPrimaryKeyRelatedField(CachedPrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context["request"].user
        return user.owned_apps.all().prefetch_related("backends")


class SharedAppPrimaryKeyRelatedField(CachedPrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context["request"].user
        return user.apps.all().prefetch_related("backends", "backends__site")


class OwnedJobPrimaryKeyRelatedField(CachedPrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context["request"].user
        return Job.objects.filter(owner=user)


class OwnedBatchJobPrimaryKeyRelatedField(CachedPrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context["request"].user
        return BatchJob.objects.filter(site__owner=user)


# USER
# ----
class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = (
            "pk",
            "url",
            "username",
            "email",
            "sites",
            "apps",
        )

    url = serializers.HyperlinkedIdentityField(view_name="user-detail")
    sites = serializers.HyperlinkedRelatedField(
        many=True, read_only=True, view_name="site-detail"
    )
    apps = serializers.HyperlinkedRelatedField(
        many=True, read_only=True, view_name="app-detail"
    )

    def create(self, validated_data):
        user = User.objects.create_user(**validated_data)
        return user


# SITE
# ----
class SiteStatusSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SiteStatus
        fields = [
            "num_nodes",
            "num_idle_nodes",
            "num_busy_nodes",
            "num_down_nodes",
            "backfill_windows",
            "queued_jobs",
        ]

    backfill_windows = serializers.ListField(
        child=serializers.ListField(
            child=serializers.IntegerField(min_value=0),
            allow_empty=False,
            min_length=2,
            max_length=2,
        ),
        allow_empty=True,
        required=False,
    )
    queued_jobs = serializers.ListField(child=serializers.DictField(), required=False)

    def _validate_queued_job(self, job):
        key_types = {
            "queue": str,
            "state": str,
            "num_nodes": int,
            "score": int,
            "queued_time_min": int,
            "wall_time_min": int,
        }
        required_keys = set(key_types.keys())
        diff = required_keys.difference(job.keys())
        if diff:
            raise ValidationError(f"Missing required keys: {diff}")
        ret = {}
        for key, typ in key_types.items():
            try:
                ret[key] = typ(job[key])
            except ValueError:
                raise ValidationError(f"{job[key]} is not a valid {typ.__name__}")
        return ret

    def validate_queued_jobs(self, value):
        return [self._validate_queued_job(v) for v in value]


class SiteSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Site
        fields = [
            "pk",
            "url",
            "hostname",
            "path",
            "last_refresh",
            "status",
            "apps",
        ]

    status = SiteStatusSerializer(required=False)
    apps = serializers.StringRelatedField(
        many=True, source="registered_app_backends", read_only=True
    )

    def create(self, validated_data):
        validated_data["owner"] = self.context["request"].user
        site = Site.objects.create(
            owner=validated_data["owner"],
            hostname=validated_data["hostname"],
            path=validated_data["path"],
        )
        return site

    def update(self, instance, validated_data):
        dat = validated_data
        instance.update(
            hostname=dat.get("hostname"), path=dat.get("path"), status=dat.get("status")
        )
        return instance

    def validate_path(self, value):
        path = Path(value)
        if not path.is_absolute():
            raise ValidationError("must be an absolute POSIX path")
        return path.as_posix()


# APP
# ----
class AppBackendSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AppBackend
        validators = []
        fields = (
            "site",
            "site_hostname",
            "site_path",
            "class_name",
        )

    site = OwnedSitePrimaryKeyRelatedField()
    site_hostname = serializers.CharField(source="site.hostname", read_only=True)
    site_path = serializers.CharField(source="site.path", read_only=True)
    class_name = serializers.CharField(
        help_text="The app class defined at {AppModule}.{AppClass}",
        max_length=128,
        required=True,
    )

    def validate_class_name(self, value):
        module, *clsname = value.split(".")
        if not module.isidentifier():
            raise ValidationError("App class_name must be a valid Python identifier")
        if not (len(clsname) == 1 and clsname[0].isidentifier()):
            raise ValidationError("App class_name must be a valid Python identifier")
        return value


class AppSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AppExchange
        fields = (
            "pk",
            "name",
            "description",
            "parameters",
            "owner",
            "users",
            "backends",
        )

    backends = AppBackendSerializer(many=True)
    parameters = serializers.ListField(child=serializers.CharField(max_length=128))
    owner = serializers.SlugRelatedField(slug_field="username", read_only=True)
    users = serializers.SlugRelatedField(
        slug_field="username", many=True, queryset=User.objects.all(), required=False
    )

    def validate_backends(self, value):
        if len(value) == 0:
            raise ValidationError("Provide at least one backend")
        return value

    def create(self, validated_data):
        dat = validated_data
        app_exchange = AppExchange.objects.create_new(
            name=dat["name"],
            description=dat.get("description", ""),
            parameters=dat["parameters"],
            backend_dicts=dat["backends"],
            owner=dat["owner"],
            users=dat.get("users", []),
        )
        return app_exchange

    def update(self, instance, validated_data):
        dat = validated_data
        instance.update(
            name=dat.get("name"),
            description=dat.get("description"),
            parameters=dat.get("parameters"),
            users=dat.get("users"),
            backend_dicts=dat.get("backends"),
        )
        return instance


class AppMergeSerializer(serializers.Serializer):
    name = serializers.CharField(min_length=1, max_length=128)
    description = serializers.CharField(required=False, allow_blank=True)
    existing_apps = OwnedAppPrimaryKeyRelatedField(many=True)

    def create(self, validated_data):
        dat = validated_data
        app_exchange = AppExchange.objects.create_merged(
            name=dat["name"],
            description=dat.get("description", None),
            existing_apps=dat["existing_apps"],
            owner=dat["owner"],
        )
        return app_exchange

    def to_representation(self, instance):
        """
        On POST response, we serialize the created App using the same AppSerializer
        """
        serializer = AppSerializer(
            instance, context={"request": self.context["request"]}
        )
        return serializer.data


# JOB
# ----
class EventLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = EventLog
        fields = ("job_id", "from_state", "to_state", "timestamp", "message")

    job_id = OwnedJobPrimaryKeyRelatedField(read_only=True)


class TransferItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = TransferItem
        fields = (
            "pk",
            "state",
            "direction",
            "task_id",
            "status_message",
            "source",
            "destination",
        )
        read_only_fields = (
            "protocol",
            "remote_netloc",
            "source_path",
            "destination_path",
            "job",
        )

    source = serializers.CharField(write_only=True)
    destination = serializers.CharField(write_only=True)


class JobSerializer(BulkModelSerializer):
    class Meta:
        model = Job
        nested_update_fields = ["transfer_items"]
        fields = (
            "pk",
            "workdir",
            "tags",
            "batch_job",
            "app",
            "app_name",
            "site",
            "app_class",
            "events",
            "transfer_items",
            "return_code",
            "last_error",
            "lock_status",
            "state",
            "state_timestamp",
            "state_message",
            "parameters",
            "data",
            "last_update",
            "parents",
            "children",
            "num_nodes",
            "ranks_per_node",
            "threads_per_rank",
            "threads_per_core",
            "cpu_affinity",
            "gpus_per_rank",
            "node_packing_count",
            "wall_time_min",
        )

    events = serializers.HyperlinkedIdentityField(
        read_only=True,
        view_name="job-event-list",
        lookup_field="pk",
        lookup_url_kwarg="job_id",
    )
    tags = serializers.DictField(child=serializers.CharField(max_length=32))
    batch_job = OwnedBatchJobPrimaryKeyRelatedField(required=False)

    # Read/Write: app pk. Read-only: name, backend-site, backend-class
    app = SharedAppPrimaryKeyRelatedField(source="app_exchange")
    app_name = serializers.StringRelatedField(
        read_only=True, source="app_exchange.name"
    )
    site = serializers.StringRelatedField(
        default=None, allow_null=True, read_only=True, source="app_backend.site.__str__"
    )
    app_class = serializers.StringRelatedField(
        default=None, allow_null=True, read_only=True, source="app_backend.class_name"
    )

    transfer_items = TransferItemSerializer(many=True, allow_empty=True)
    parents = OwnedJobPrimaryKeyRelatedField(many=True)
    children = OwnedJobPrimaryKeyRelatedField(many=True, read_only=True)
    parameters = serializers.DictField(child=serializers.CharField(max_length=128))
    data = serializers.DictField()
    lock_status = serializers.CharField(read_only=True, source="get_lock_status")
    state_timestamp = serializers.DateTimeField(write_only=True, required=False)
    state_message = serializers.CharField(write_only=True, required=False)

    num_nodes = serializers.IntegerField(min_value=1)

    def create(self, validated_data):
        return Job.objects.create(**validated_data)

    def update(self, instance, validated_data):
        return instance.update(**validated_data)

    def validate_workdir(self, value):
        path = Path(value)
        if path.is_absolute():
            raise ValidationError("must be a relative POSIX path")
        return path.as_posix()


# BATCHJOB
# ---------
class BatchJobSerializer(BulkModelSerializer):
    class Meta:
        model = BatchJob
        fields = (
            "pk",
            "site",
            "scheduler_id",
            "project",
            "queue",
            "num_nodes",
            "wall_time_min",
            "job_mode",
            "filter_tags",
            "state",
            "status_message",
            "start_time",
            "end_time",
            "revert",
        )

    revert = serializers.BooleanField(required=False, default=False, write_only=True)
    site = OwnedSitePrimaryKeyRelatedField()
    filter_tags = serializers.DictField(child=serializers.CharField(max_length=32))

    def create(self, validated_data):
        dat = validated_data
        return BatchJob.objects.create(
            site=dat["site"],
            project=dat["project"],
            queue=dat["queue"],
            num_nodes=dat["num_nodes"],
            wall_time_min=dat["wall_time_min"],
            job_mode=dat["job_mode"],
            filter_tags=dat["filter_tags"],
        )

    def update(self, instance, validated_data):
        validated_data.pop("site", None)
        validated_data.pop("pk", None)
        instance.update(**validated_data)
        return instance

    def validate_num_nodes(self, value):
        if value < 1:
            raise ValidationError("num_nodes must be greater than 0")
        return value

    def validate_wall_time_min(self, value):
        if value < 1:
            raise ValidationError("num_nodes must be greater than 0")
        return value


class SessionSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobLock
        fields = ("pk", "heartbeat", "label", "site", "batch_job")

    def create(self, validated_data):
        return JobLock.objects.create(
            site=validated_data["site"],
            label=validated_data["label"],
            batch_job=validated_data["batch_job"],
        )

    def update(self, instance, validated_data):
        """Only ticks lock"""
        instance.tick()
        return instance


class NodeResourceSerializer(serializers.Serializer):
    """
    Launchers provide a snapshot of current resources in the call to acquire()
    new jobs for execution:
    {
        "max_jobs_per_node": 1,  # determined by site and job mode
        "max_wall_time_min": 60,
        "running_job_counts": [0, 1, 0],
        "node_occupancies": [0.0, 1.0, 0.0],
        "idle_cores": [64, 63, 64],
        "idle_gpus": [1, 0, 1],
    }
    """

    max_jobs_per_node = serializers.IntegerField(min_value=1, max_value=256)
    max_wall_time_min = serializers.IntegerField(min_value=0)
    running_job_counts = serializers.ListField(
        allow_empty=False, child=serializers.IntegerField(min_value=0, max_value=256)
    )
    node_occupancies = serializers.ListField(
        allow_empty=False, child=serializers.FloatField(min_value=0.0, max_value=1.0)
    )
    idle_cores = serializers.ListField(
        allow_empty=False, child=serializers.IntegerField(min_value=0, max_value=256)
    )
    idle_gpus = serializers.ListField(
        allow_empty=False, child=serializers.IntegerField(min_value=0, max_value=256)
    )

    def validate(self, data):
        num_nodes_set = set(
            map(
                len,
                [
                    data["running_job_counts"],
                    data["node_occupancies"],
                    data["idle_cores"],
                    data["idle_gpus"],
                ],
            )
        )
        if len(num_nodes_set) != 1:
            raise ValidationError(
                "All lists must have the same length equal to the number of nodes."
            )
        return data


class JobAcquireSerializer(serializers.Serializer):
    acquire_unbound = serializers.BooleanField()
    states = serializers.MultipleChoiceField(choices=JOB_STATE_CHOICES)
    max_num_acquire = serializers.IntegerField(min_value=0, max_value=10_000)
    filter_tags = serializers.DictField(
        child=serializers.CharField(max_length=32),
        allow_empty=True,
        required=False,
        default=None,
    )
    node_resources = NodeResourceSerializer(
        required=False, allow_null=True, default=None
    )
    order_by = serializers.ListField(
        allow_empty=True,
        child=serializers.CharField(max_length=32),
        required=False,
        allow_null=True,
        default=(),
    )

    def to_representation(self, acquired_jobs):
        """Re-use JobSerializer for Response: the acquired jobs"""
        serializer = JobSerializer(
            acquired_jobs, many=True, context={"request": self.context["request"]}
        )
        return {"acquired_jobs": serializer.data}

    def create(self, validated_data):
        return Job.objects.acquire(**validated_data)

    def update(self, instance, validated_data):
        raise NotImplementedError("This serializer is only hit by POST")

    def validate_order_by(self, value):
        if not value:
            return []
        order_fields = [
            "num_nodes",
            "node_packing_count",
            "wall_time_min",
        ]
        order_fields.extend("-" + f for f in order_fields[:])
        for field in value:
            if field not in order_fields:
                raise ValidationError(
                    f"Invalid order_by field: {field}. Must choose from: {order_fields}"
                )
        return value
