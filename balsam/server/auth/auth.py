from django.utils.translation import gettext_lazy as _
from rest_framework import authentication, exceptions
from .models import Site

class SiteTokenAuthentication(authentication.TokenAuthentication):
    """
    Balsam Site Token based authentication. 
    A User owns several Sites, and each Site is associated with a Token.
    The auth context thus includes the Site from which a User has authenticated.
    Clients should authenticate by passing the token key in the "Authorization"
    HTTP header, prepended with the string "Token ".  For example:
        Authorization: Token 401f7ac837da42b97f613d789819ff93537bee6a
    """
    def authenticate_credentials(self, key):
        model = Site
        try:
            site = model.objects.select_related('owner').get(token=key)
        except model.DoesNotExist:
            raise exceptions.AuthenticationFailed(_('Invalid token.'))

        if not site.owner.is_active:
            raise exceptions.AuthenticationFailed(_('User inactive or deleted.'))

        return (site.owner, site)
