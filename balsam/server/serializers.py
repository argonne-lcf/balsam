from rest_framework import serializers

from balsam.server.models import (
    User,
    AppExchange,
    AppBackend
    Site,
    SitePolicy,
    SiteStatus,
    BatchJob,
    Job,
    TransferItem,
    EventLog,
)

class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = (
            'pk', 'url', 'username', 'password', 'email',
            'sites', 'apps',
        )
    url = serializers.HyperlinkedIdentityField(view_name='user_detail')
    password = serializers.CharField(max_length=128, write_only=True, required=False)
    sites = serializers.HyperlinkedRelatedField(
        many=True, read_only=True,
        view_name='site_detail'
    )
    apps = serializers.HyperlinkedRelatedField(
        many=True, read_only=True,
        view_name='site_detail'
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

class SiteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Site
        read_only_fields = ['owner']
        fields = [
            'pk', 'url', 'hostname', 'site_path',
            'heartbeat', 'owner', 'status', 'policy',
        ]

    status = SiteStatusSerializer()
    policy = SitePolicySerializer()

    def create(self, validated_data):
        owner = self.context['request'].user
        site = Site.objects.create(
            owner = owner,
            **validated_data
        )
        return site
    
    def update(self, instance, validated_data):
        return instance.update(**validated_data)
    
    def validate_site_path(self, value):
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
            'site',
            'hostname',
            'path'
            'class_name'
        )

    hostname = serializers.CharField(
        source='site.hostname',
        read_only=True
    )
    class_name = serializers.CharField(
        source='site.class_name',
        read_only=True
    )

class AppSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = AppExchange
        fields = (
            'name', 
            'description', 
            'parameters', 
            'owner', 
            'users', 
            'backends'
        )
    
    backends = AppBackendSerializer(many=True)

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
            'events', 'transfers', 
            'app', 'state',
            'last_update', 'data', 'placeholders', 'parents'
        )
    transfer_items = TransferItemSerializer(many=True)
    parents = serializers.HyperlinkedRelatedField(many=True)

class BatchJobSerializer(serializers)
    class Meta:
        model = SchedulerJob
        fields = (
            'site', 'scheduler_id', 'project', 'queue',
            'nodes', 'wall_time_min', 'job_mode',
            'filter_tags', 'state',
            'start_time', 'end_time'
        )