from rest_framework import viewsets, mixins
from rest_framework import permissions, status
from rest_framework.decorators import api_view, action
from rest_framework.response import Response
from rest_framework.reverse import reverse

#from ..auth import SiteTokenAuthentication
#from ..permissions import IsOwnerOrReadOnly, IsOwner
from .. import serializers
from django.contrib.auth import get_user_model
from django.shortcuts import redirect
#from ..models import (
#    Site,
#    App,
#    Job,
#    Task,
#    SchedulerJob,
#    User,
#)

@api_view(['GET'])
def api_root(request):
    return redirect(
        reverse('user_detail', [request.user.pk], request=request)
    )

#class SiteViewSet(viewsets.GenericViewSet, 
#                  mixins.ListModelMixin, 
#                  mixins.RetrieveModelMixin,
#                  mixins.UpdateModelMixin,
#                  mixins.DestroyModelMixin):
#    queryset = Site.objects.all()
#    serializer_class = serializers.SiteSerializer
#    permission_classes = (permissions.IsAuthenticated,IsOwnerOrReadOnly)
#
#    def get_queryset(self):
#        user = self.request.user
#        if user.is_staff:
#            return Site.objects.all()
#        return user.authorized_sites.all()
#
#    @action(detail=False, methods=['POST'], permission_classes=[permissions.IsAuthenticated])
#    def register(self, request):
#        new_site = Site.objects.register(owner=request.user)
#        return Response({"token": new_site.token}, status=status.HTTP_200_OK)
#
#    @action(detail=False, methods=['POST'], authentication_classes=[SiteTokenAuthentication])
#    def activate(self, request):
#        site = request.auth
#        if site.activated:
#            return Response({'errors': 'already activated'}, status=status.HTTP_400_BAD_REQUEST)
#        try:
#            hostname = request.data['hostname']
#            site_path = request.data['site_path']
#        except KeyError:
#            return Response({'errors': 'missing hostname or site_path'}, status=HTTP_400_BAD_REQUEST)
#        site.activate(hostname, site_path)
#        ser = serializers.SiteSerializer(site, context={'request': request})
#        return Response(ser.data, status=status.HTTP_201_CREATED)
#
#    @action(detail=True, permission_classes=[IsOwner])
#    def reset_token(self, request, pk=None):
#        site = self.get_object()
#        new_token = site.reset_token()
#        tr = {"token": new_token}
#        return Response(tr)
#
#class AppViewSet(viewsets.ModelViewSet):
#    queryset = App.objects.all()
#    serializer_class = serializers.AppSerializer
#    permission_classes = (permissions.IsAuthenticated,)
#
#    def get_queryset(self):
#        user = self.request.user
#        if user.is_staff:
#            return App.objects.all()
#        return App.objects.filter(site__in=user.authorized_sites.all())
#
#class JobViewSet(viewsets.ModelViewSet):
#    queryset = Job.objects.all()
#    serializer_class = serializers.JobSerializer
#    permission_classes = (permissions.IsAuthenticated,)
#
#    def get_queryset(self):
#        user = self.request.user
#        if user.is_staff:
#            return Job.objects.all()
#        return Job.objects.filter(site__in=user.authorized_sites.all())
#
#class TaskViewSet(viewsets.ModelViewSet):
#    queryset = Task.objects.all()
#    serializer_class = serializers.TaskSerializer
#    permission_classes = (permissions.IsAuthenticated,)
#
#    def get_queryset(self):
#        user = self.request.user
#        if user.is_staff:
#            return Task.objects.all()
#        return Task.objects.filter(job__site__in=user.authorized_sites.all())
#
#class SchedulerJobViewSet(viewsets.ModelViewSet):
#    queryset = SchedulerJob.objects.all()
#    serializer_class = serializers.SchedulerJobSerializer
#    permission_classes = (permissions.IsAuthenticated,)
#
#    def get_queryset(self):
#        user = self.request.user
#        if user.is_staff:
#            return SchedulerJob.objects.all()
#        return SchedulerJob.objects.filter(site__in=user.authorized_sites.all())
#