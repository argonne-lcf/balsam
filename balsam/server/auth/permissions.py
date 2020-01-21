from rest_framework import permissions
from .models import Site

class IsOwnerOrReadOnly(permissions.BasePermission):
    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True
        return obj.owner == request.user

class IsOwner(permissions.BasePermission):
    def has_object_permission(self, request, view, obj):
        return obj.owner == request.user

class BelongsToSite(permissions.BasePermission):
    def has_permission(self, request, view):
        return isinstance(request.auth, Site)

    def has_object_permission(self, request, view, obj):
        if request.auth is None or not isinstance(request.auth, Site):
            return False
        authenticated_site = request.auth
        return obj.site == authenticated_site
