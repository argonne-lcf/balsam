from rest_framework import generics, permissions
from balsam.server.serializers import UserSerializer
from django.contrib.auth import get_user_model
from django.shortcuts import get_object_or_404

User = get_user_model()

class IsAuthenticatedOrAdmin(permissions.BasePermission):
    """
    Admin can see all Users; Users only see self
    """
    def has_permission(self, request, view):
        if request.user.is_staff:
            return True
        return view.kwargs["pk"] == request.user.pk

class UserList(generics.ListCreateAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAdminUser]

class UserDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAdminUser]
    permission_classes = [IsAuthenticatedOrAdmin]