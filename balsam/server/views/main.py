from rest_framework import generics, permissions
from rest_framework.decorators import api_view
from balsam.server import serializers as ser
from balsam.server.models import Site, AppExchange
from django.contrib.auth import get_user_model
from django.shortcuts import get_object_or_404
from django.shortcuts import redirect
from rest_framework.reverse import reverse

from knox.views import LoginView as KnoxLoginView
from rest_framework.authentication import BasicAuthentication

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

class LoginView(KnoxLoginView):
    authentication_classes = [BasicAuthentication]

class UserList(generics.ListCreateAPIView):
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

class SiteDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = Site.objects.all()
    serializer_class = ser.SiteSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return user.sites.all()

class AppList(generics.ListCreateAPIView):
    queryset = AppExchange.objects.all()
    serializer_class = ser.AppSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return user.apps.all()

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)

    def perform_destroy(self, instance):
        instance.delete_app()

class AppDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = AppExchange.objects.all()
    serializer_class = ser.AppSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        user = self.request.user
        return user.apps.all()

class AppMerge(generics.CreateAPIView):
    queryset = AppExchange.objects.all()
    serializer_class = ser.AppMergeSerializer
    permission_classes = [permissions.IsAuthenticated]

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user)