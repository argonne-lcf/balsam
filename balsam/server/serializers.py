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
    TransferItem,
    EventLog,
)

# OWNER-AWARE FIELDS
# ------------------
class OwnedSitePrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context['request'].user
        return Site.objects.filter(owner=user)

class OwnedAppPrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context['request'].user
        return AppExchange.objects.filter(owner=user)

class OwnedJobPrimaryKeyRelatedField(serializers.PrimaryKeyRelatedField):
    def get_queryset(self):
        user = self.context['request'].user
        return Job.objects.filter(owner=user)


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
         ), allow_empty=True
    )
    queued_jobs = serializers.ListField(
        child=serializers.DictField(child=serializers.CharField())
    )

    def validate(self, attrs):
        num_nodes = attrs['num_nodes']
        num_idle = attrs['num_idle_nodes']
        num_busy = attrs['num_busy_nodes']
        if num_nodes < 1:
            raise serializers.ValidationError('num_nodes must be at least 1')
        if num_idle < 0 or num_busy < 0:
            raise serializers.ValidationError('Cannot have negative node count')
        if num_idle + num_busy > num_nodes:
            raise serializers.ValidationError(
                'num_idle_nodes+num_busy_nodes cannot exceed num_nodes'
            )
        for (num_backfill_nodes, _) in attrs['backfill_windows']:
            if num_backfill_nodes > num_idle:
                raise serializers.ValidationError(
                    'Backfill nodes cannot exceed # of idle nodes'
                )
        return attrs

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
    status = SiteStatusSerializer()
    apps = serializers.StringRelatedField(
        many=True, source='registered_app_backends')

    def create(self, validated_data):
        validated_data["owner"] = self.context['request'].user
        site = Site.objects.create(**validated_data)
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
            raise serializers.ValidationError('must be an absolute POSIX path')
        return path.as_posix()
    

# APP
# ----
class AppBackendSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AppBackend
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
            raise serializers.ValidationError('App class_name must be a valid Python identifier')
        if not (len(clsname)==1 and clsname[0].isidentifier()):
            raise serializers.ValidationError('App class_name must be a valid Python identifier')
        return value

class AppSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AppExchange
        fields = (
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
        many=True, queryset=User.objects.all()
    )
    user_urls = serializers.HyperlinkedRelatedField(
        source='users', read_only=True,
        view_name='user-detail', many=True
    )

    def validate_backends(self, value):
        if len(value) == 0:
            raise serializers.ValidationError("Provide at least one backend")
        return value

    def create(self, validated_data):
        dat = validated_data
        app_exchange = AppExchange.objects.create_new(
            name=dat["name"],
            description=dat["description"],
            parameters=dat["parameters"],
            backend_dicts=dat["backends"],
            owner=dat["owner"],
            users=dat["users"],
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
    description = serializers.CharField(allow_blank=True)
    existing_apps = OwnedAppPrimaryKeyRelatedField(many=True)

    def create(self, validated_data):
        dat = validated_data
        app_exchange = AppExchange.objects.create_merged(
            name=dat["name"],
            description=dat["description"],
            existing_apps=dat["existing_apps"],
            owner=dat["owner"]
        )
        return app_exchange

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
            'source', 'destination', 'job'
        )

class JobSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Job
        fields = (
            'workdir', 'tags',
            'site', 'owner', 'batch_job',
            'app_class', 'app_name', 'app_id',
            'events', 'transfer_items',
            'state', 'parameters', 'data',
            'last_update', 'parents',
            'num_nodes', 'ranks_per_node', 'threads_per_rank',
            'threads_per_core', 'cpu_affinity', 'gpus_per_rank',
            'node_packing_count', 'wall_time_min'
        )
    transfer_items = TransferItemSerializer(many=True)
    parents = OwnedJobPrimaryKeyRelatedField(many=True)
    parent_urls = serializers.HyperlinkedRelatedField(
        many=True, view_name = 'job-detail', read_only=True
    )
    tags = serializers.DictField(
        child=serializers.CharField(max_length=32)
    )

    def validate_workdir(self, value):
        path = Path(value)
        if path.is_absolute():
            raise serializers.ValidationError('must be a relative POSIX path')
        return path.as_posix()

# BATCHJOB
# ---------
class BatchJobListSerializer(serializers.ListSerializer):
    def update(self, instance, validated_data):
        """
        Bulk partial-update: no creation/deletion/reordering
        """
        allowed_pks = instance.values_list('pk', flat=True)
        patch_list = [
            patch for patch in validated_data
            if patch["pk"] in allowed_pks
        ]
        BatchJob.objects.bulk_update(patch_list)
        
class BatchJobSerializer(serializers.HyperlinkedModelSerializer):
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
    # we need "pk" as writeable field for bulk-updates
    pk = serializers.IntegerField(allow_blank=True)
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
        view_name="batchjob-ensemble-list"
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
            raise serializers.ValidationError('num_nodes must be greater than 0')
        return value

    def validate_wall_time_min(self, value):
        if value < 1:
            raise serializers.ValidationError('num_nodes must be greater than 0')
        return value