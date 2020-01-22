from pathlib import Path
from rest_framework import serializers

from balsam.server.models import (
    User,
    AppExchange,
    AppBackend,
    Site,
    SitePolicy,
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


# SERIALIZERS
# -----------
class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = (
            'pk', 'url', 'username', 'password', 'email',
            'sites', 'apps',
        )
    url = serializers.HyperlinkedIdentityField(view_name='user-detail')
    password = serializers.CharField(max_length=128, write_only=True, required=False)
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

class SiteStatusSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SiteStatus
        fields = [
            'num_nodes', 'num_idle_nodes', 'num_busy_nodes',
            'num_down_nodes', 'backfill_windows', 'queued_jobs',
            'reservations',
        ]

class SitePolicySerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SitePolicy
        fields = [
            'submission_mode', 'max_num_queued', 'min_num_nodes',
            'max_num_nodes', 'min_wall_time_min', 'max_wall_time_min',
            'max_total_node_hours', 'node_hours_used',
        ]

class SiteSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Site
        read_only_fields = ['owner']
        fields = [
            'pk', 'url', 'hostname', 'path',
            'heartbeat', 'owner', 'status', 'policy',
            'apps',
        ]

    status = SiteStatusSerializer()
    policy = SitePolicySerializer()
    apps = serializers.StringRelatedField(
        many=True, source='registered_app_backends')

    def create(self, validated_data):
        owner = self.context['request'].user
        site = Site.objects.create(
            owner = owner,
            **validated_data
        )
        return site
    
    def update(self, instance, validated_data):
        return instance.update(**validated_data)
    
    def validate_path(self, value):
        if not value.startswith('/'):
            raise serializers.ValidationError(
                "Must provide absolute path, starting with '/'.")
        return value
    
    def validate_owner(self, value):
        current_user = self.context['request'].user
        if current_user in value:
            raise serializers.ValidationError(
                "Owner cannot add themselves to authorized_users group")
        return value

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
        view_name='user-detail',
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

class BatchJobSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = BatchJob
        fields = (
            'site', 'scheduler_id', 'project', 'queue',
            'nodes', 'wall_time_min', 'job_mode',
            'filter_tags', 'state',
            'start_time', 'end_time'
        )