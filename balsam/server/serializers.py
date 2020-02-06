from pathlib import Path
from urllib.parse import urlencode
from rest_framework import serializers
from rest_framework.reverse import reverse

from balsam.server.models import (
    User,
    AppExchange,
    AppBackend,
    Site,
    SiteStatus,
    BatchJob,
    Job,
    TransferItem,
    EventLog,
)

ValidationError = serializers.ValidationError

# OWNER-AWARE FIELDS
# ------------------
class OwnedSitePrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context['request'].user
        return Site.objects.filter(owner=user)

class OwnedAppPrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context['request'].user
        return user.owned_apps.all()

class SharedAppPrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context['request'].user
        return user.apps.all()

class OwnedJobPrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context['request'].user
        return Job.objects.filter(owner=user)

class OwnedBatchJobPrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def get_queryset(self):
        ser = self.context['request'].user
        return BatchJob.objects.filter(site__owner=user)

class QueryParameterHyperlinkedIdentityField(serializers.HyperlinkedIdentityField):
    def get_url(self, obj, view_name, request, format):
        lookup_field_value = getattr(obj, self.lookup_field, None)
        result = '{}?{}'.format(
            reverse(view_name, kwargs={}, request=request, format=format),
            urlencode({self.lookup_url_kwarg: lookup_field_value})
        )
        return result

# USER
# ----
class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = (
            'pk', 'url', 'username', 'email', 'sites', 'apps',
        )
    url = serializers.HyperlinkedIdentityField(view_name='user-detail')
    sites = serializers.HyperlinkedRelatedField(
        many=True, read_only=True,
        view_name='site-detail'
    )
    apps = serializers.HyperlinkedRelatedField(
        many=True, read_only=True,
        view_name='app-detail'
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
            'num_nodes', 'num_idle_nodes', 'num_busy_nodes',
            'num_down_nodes', 'backfill_windows', 'queued_jobs',
        ]

    backfill_windows = serializers.ListField(
        child=serializers.ListField(
            child=serializers.IntegerField(min_value=0),
            allow_empty=False, min_length=2, max_length=2
         ), allow_empty=True, required=False
    )
    queued_jobs = serializers.ListField(
        child=serializers.DictField(),
        required=False
    )

    def _validate_queued_job(self, job):
        key_types = {
            'queue': str, 'state': str,
            'num_nodes': int, 'score': int, 'queued_time_min': int,
            'wall_time_min': int
        }
        required_keys = set(key_types.keys())
        diff = required_keys.difference(job.keys())
        if diff:
            raise ValidationError(f'Missing required keys: {diff}')
        ret = {}
        for key, typ in key_types.items():
            try:
                ret[key] = typ(job[key])
            except ValueError: 
                raise ValidationError(f'{job[key]} is not a valid {typ.__name__}')
        return ret

    def validate_queued_jobs(self, value):
        return [self._validate_queued_job(v) for v in value]


class SiteSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Site
        read_only_fields = ['owner', 'owner_url']
        fields = [
            'pk', 'url', 'hostname', 'path',
            'last_refresh', 'owner', 'owner_url', 'status', 'apps',
        ]

    owner = serializers.PrimaryKeyRelatedField(read_only=True)
    owner_url = serializers.HyperlinkedRelatedField(
        source='owner', read_only=True,
        view_name='user-detail',
    )
    status = SiteStatusSerializer(required=False)
    apps = serializers.StringRelatedField(
        many=True, source='registered_app_backends',
        read_only=True
    )

    def create(self, validated_data):
        validated_data["owner"] = self.context['request'].user
        site = Site.objects.create(
            owner=validated_data['owner'],
            hostname=validated_data['hostname'],
            path=validated_data['path']
        )
        return site
    
    def update(self, instance, validated_data):
        dat = validated_data
        instance.update(
            hostname=dat.get('hostname'),
            path=dat.get('path'),
            status=dat.get('status')
        )
        return instance
    
    def validate_path(self, value):
        path = Path(value)
        if not path.is_absolute():
            raise ValidationError('must be an absolute POSIX path')
        return path.as_posix()
    

# APP
# ----
class AppBackendSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AppBackend
        validators = []
        fields = (
            'site', 'site_url', 'site_hostname', 'site_path', 'class_name',
        )

    site = OwnedSitePrimaryKeyRelatedField()
    site_url = serializers.HyperlinkedRelatedField(
        source='site', read_only=True, view_name='site-detail'
    )
    site_hostname = serializers.CharField(
        source='site.hostname',
        read_only=True
    )
    site_path = serializers.CharField(
        source='site.path',
        read_only=True
    )
    class_name = serializers.CharField(
        help_text='The app class defined at {AppModule}.{AppClass}',
        max_length=128, required=True
    )

    def validate_class_name(self, value):
        module, *clsname = value.split('.')
        if not module.isidentifier():
            raise ValidationError('App class_name must be a valid Python identifier')
        if not (len(clsname)==1 and clsname[0].isidentifier()):
            raise ValidationError('App class_name must be a valid Python identifier')
        return value

class AppSerializer(serializers.HyperlinkedModelSerializer):

    class Meta:
        model = AppExchange
        fields = (
            'pk',
            'name',
            'description',
            'parameters',
            'owner',
            'owner_url',
            'users',
            'user_urls',
            'backends'
        )
    
    backends = AppBackendSerializer(many=True)
    parameters = serializers.ListField(
        child=serializers.CharField(max_length=128)
    )
    owner = serializers.PrimaryKeyRelatedField(read_only=True)
    owner_url = serializers.HyperlinkedRelatedField(
        source='owner', read_only=True,
        view_name='user-detail',
    )
    users = serializers.PrimaryKeyRelatedField(
        many=True, queryset=User.objects.all(),
        required=False
    )
    user_urls = serializers.HyperlinkedRelatedField(
        source='users', read_only=True,
        view_name='user-detail', many=True
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
            users=dat.get("users", [])
        )
        return app_exchange

    def update(self, instance, validated_data):
        dat = validated_data
        instance.update(
            name=dat.get("name"),
            description=dat.get("description"),
            parameters=dat.get("parameters"),
            users=dat.get("users"),
            backend_dicts=dat.get("backends")
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
            owner=dat["owner"]
        )
        return app_exchange

    def to_representation(self, instance):
        """
        On POST response, we serialize the created App using the same AppSerializer
        """
        serializer = AppSerializer(
            instance,
            context={"request": self.context["request"]}
        )
        return serializer.data

# JOB
# ----
class EventLogSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = EventLog
        fields = (
            'job',
            'from_state',
            'to_state',
            'timestamp',
            'message'
        )

class TransferItemSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = TransferItem
        fields = (
            'protocol', 'state', 'direction',
            'source', 'destination',
            'task_id', 'status_message'
        )

class JobListSerializer(serializers.ListSerializer):
    def create(self, validated_data):
        pass
        

class JobSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Job
        fields = (
            'workdir', 'tags', 'owner', 'batch_job',
            'app', 'app_url', 'app_name', 'site', 'app_class',
            'events', 'transfer_items',
            'state', 'parameters', 'data',
            'last_update', 'parents', 'parent_urls',
            'num_nodes', 'ranks_per_node', 'threads_per_rank',
            'threads_per_core', 'cpu_affinity', 'gpus_per_rank',
            'node_packing_count', 'wall_time_min'
        )

    def __init__(self, *args, **kwargs):
        show_events = kwargs.pop('show_events', False)
        super().__init__(*args, **kwargs)
        if show_events:
            self.fields['events'] = EventLogSerializer(many=True, read_only=True)
        else:
            self.fields['events'] = serializers.HyperlinkedIdentityField(
                read_only=True, view_name='job-event-list', lookup_field='pk',
                lookup_url_kwarg='job_id'
            )

    tags = serializers.DictField(child=serializers.CharField(max_length=32))
    batch_job = OwnedBatchJobPrimaryKeyRelatedField()

    # Read/Write: app pk. Read-only: App URL, name, backend-site, backend-class
    app = SharedAppPrimaryKeyRelatedField(source='app_exchange')
    app_url = serializers.HyperlinkedRelatedField(
        view_name='app-detail', read_only=True, source='app_exchange'
    )
    app_name = serializers.StringRelatedField(
        read_only=True, source='app_exchange.name'
    )
    site = serializers.ReadOnlyField()
    app_class = serializers.ReadOnlyField()

    transfer_items = TransferItemSerializer(many=True)
    parents = OwnedJobPrimaryKeyRelatedField(many=True)
    parent_urls = serializers.HyperlinkedRelatedField(
        many=True, view_name='job-detail', read_only=True
    )
    parameters = serializers.DictField(
        child=serializers.CharField(max_length=128)
    )
    data = serializers.DictField()

    def validate_workdir(self, value):
        path = Path(value)
        if path.is_absolute():
            raise ValidationError('must be a relative POSIX path')
        return path.as_posix()

# BATCHJOB
# ---------
class BatchJobListSerializer(serializers.ListSerializer):
    def update(self, instance, validated_data):
        """
        Bulk partial-update: no creation/deletion/reordering
        """
        allowed_pks = instance.values_list('pk', flat=True)
        patch_list = []
        for patch in validated_data:
            pk = patch['pk']
            if pk in allowed_pks:
                patch_list.append(patch)
            else:
                raise ValidationError(f'Invalid pk: {pk}')
        jobs = BatchJob.objects.bulk_update(patch_list)
        return jobs
        
class BatchJobSerializer(serializers.HyperlinkedModelSerializer):
    def __init__(self, *args, **kwargs):
        bulk_update = kwargs.pop('bulk_update', False)
        super().__init__(*args, **kwargs)

        # we need "pk" and "revert" as writeable fields for bulk-updates
        if bulk_update:
            self.fields['pk'] = serializers.IntegerField(required=True)
            self.fields['revert'] = serializers.BooleanField(
                required=False, default=False, write_only=True
            )

    class Meta:
        model = BatchJob
        list_serializer_class = BatchJobListSerializer
        fields = (
            'pk', 'url', 'site', 'site_url',
            'scheduler_id', 'project', 'queue',
            'num_nodes', 'wall_time_min', 'job_mode',
            'filter_tags', 'state', 'status_message',
            'start_time', 'end_time', 'jobs'
        )
    site = OwnedSitePrimaryKeyRelatedField()
    site_url = serializers.HyperlinkedRelatedField(
        source='site', read_only=True, view_name='site-detail'
    )
    filter_tags = serializers.DictField(
        child=serializers.CharField(max_length=32)
    )
    # Including job URLs may result in fetching millions of rows
    # Instead, provide a nested URL to access the Job collection:
    jobs = serializers.HyperlinkedIdentityField(
        view_name="batchjob-ensemble-list",
        lookup_field="pk",
        lookup_url_kwarg="batch_job_id"
    )

    def create(self, validated_data):
        dat = validated_data
        return BatchJob.objects.create(
            site=dat['site'],
            project=dat['project'],
            queue=dat['queue'],
            num_nodes=dat['num_nodes'],
            wall_time_min=dat['wall_time_min'],
            job_mode=dat['job_mode'],
            filter_tags=dat['filter_tags'],
        )

    def update(self, instance, validated_data):
        validated_data.pop('site', None)
        validated_data.pop('pk', None)
        instance.update(**validated_data)
        return instance

    def validate_num_nodes(self, value):
        if value < 1:
            raise ValidationError('num_nodes must be greater than 0')
        return value

    def validate_wall_time_min(self, value):
        if value < 1:
            raise ValidationError('num_nodes must be greater than 0')
        return value