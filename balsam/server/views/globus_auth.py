import os
import requests
from urllib.parse import urlencode

from django.shortcuts import redirect, get_object_or_404
from django.contrib.auth import get_user_model, login, logout

from rest_framework.reverse import reverse as drf_reverse
from rest_framework import decorators, exceptions, status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework import viewsets
from ..models import LoginAttempt
from .. import serializers

TOKEN_URL = 'https://auth.globus.org/v2/oauth2/token'
AUTHORIZE_URL = 'https://auth.globus.org/v2/oauth2/authorize'

def reverse(view_name, request, args=()):
    return drf_reverse(view_name, args, request=request).replace('http://', 'https://')

def split_name(s):
    first = ' '.join(s.split()[:-1])
    last = ' '.join(s.split()[-1:])
    if not first: first,last = last,first
    return first,last

def globus_credential():
    '''For HTTP Basic Auth requests to Globus'''
    client_id = os.environ['GLOBUS_CLIENT_ID']
    client_secret = os.environ['GLOBUS_CLIENT_SECRET']
    return client_id, client_secret

def make_query_url(base, **kwargs):
    return base + '?' + urlencode(kwargs)

def get_redirect_uri(request):
    return reverse('globus-callback', request)

def fetch_token(request, auth_code, state):
    '''Trade authorization code for access token'''
    url = make_query_url(
        TOKEN_URL, 
        grant_type="authorization_code",
        code=auth_code,
        redirect_uri=get_redirect_uri(request)
    )
    token_response = requests.post(url, auth=globus_credential())
    token_data = token_response.json()
    if token_data["state"] != state:
        raise exceptions.AuthenticationFailed(f'CSRF state token invalid')
    return token_data

def fetch_identity(access_token):
    '''Use access token to fetch user identity'''
    header = {'Authorization': f'Bearer {access_token}'}
    url = f'https://auth.globus.org/v2/oauth2/userinfo'
    resp = requests.get(url, headers=header)
    user_info = resp.json()

    identity = {}
    first_name, last_name = split_name(user_info['name'])
    identity['globus_uuid'] = user_info['sub']
    identity['username'] = user_info['preferred_username']
    identity['first_name'] = first_name
    identity['last_name'] = last_name
    identity['email'] = user_info['email']
    return identity

class GlobusAuthViewSet(viewsets.ViewSet):

    @decorators.action(detail=False, methods=['get'],
                       permission_classes=[AllowAny])
    def login(self, request):
        '''Redirect user to Globus OAuth login page'''
        scopes = (
            'offline_access',
            'urn:globus:auth:scope:transfer.api.globus.org:all',
            'openid',
            'profile',
            'email',
        )
        params = dict(
            client_id=globus_credential()[0],
            redirect_uri=get_redirect_uri(request),
            scope=' '.join(scopes),
            state=LoginAttempt.objects.create(),
            response_type="code",
        )
        url = make_query_url(AUTHORIZE_URL, **params)
        return redirect(url)
    
    @decorators.action(detail=False, methods=['get'],
                       permission_classes=[AllowAny])
    def logout(self, request):
        '''Logout user'''
        logout(request)
        return redirect(reverse('api-root', request))

    @decorators.action(detail=False, methods=['GET'], 
                       url_path='login/callback',
                       permission_classes=[AllowAny])
    def callback(self, request):
        '''Redirect after successful login authorization on Globus'''
        auth_code = request.query_params.get('code', None)
        state = request.query_params.get('state', None)
        if state is None or auth_code is None:
            raise exceptions.AuthenticationFailed(f'Bad query_params')

        login_attempt = get_object_or_404(LoginAttempt, state=state)
        login_attempt.delete()

        token_data = fetch_token(request, auth_code, state)
        identity = fetch_identity(token_data["access_token"])

        User = get_user_model()
        current_user = User.objects.from_globus(identity, token_data)
        login(request, current_user)
        return redirect(reverse(f'user-detail', request, args=[current_user.pk]))
    
    def list(self, request):
        return Response({
            "login": reverse('globus-login', request), 
            "logout": reverse('globus-logout', request)
        })
