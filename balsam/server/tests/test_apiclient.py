'''APIClient-driven tests'''

from rest_framework.reverse import reverse
from rest_framework import status
from rest_framework.test import APITestCase, APIClient
from balsam.server.serializers import UserSerializer
from balsam.server.models import User, Site

class UserTests(APITestCase):

    @classmethod
    def setUpTestData(cls):
        bob = User.objects.create_user(username='Bob', email='bob@aol.com', password='abc')
        joe = User.objects.create_user(username='Joe', email='joe@aol.com', password='123')

    @classmethod
    def setUp(cls):
        cls.bob_client = APIClient()
        cls.bob_client.login(username='Bob', password='abc')
        cls.joe_client = APIClient()
        cls.joe_client.login(username='Joe', password='123')
    
    def test_unauthorized_user_cannot_access_sites(self):
        """Log out, then attempt app-list. Must be forbidden."""
        self.bob_client.logout()
        url = reverse('site-list')
        response = self.bob_client.get(url)
        self.assertIn(response.status_code, [status.HTTP_403_FORBIDDEN, status.HTTP_401_UNAUTHORIZED])
    
    def test_authorized_user_can_access_apps(self):
        """Can list apps"""
        url = reverse('site-list')
        response = self.bob_client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
    
    def test_shared_app(self):
        url = reverse('site-list')
        resp = self.bob_client.post(url, {'hostname': 'baz', 'path': '/foo'})
        print(resp.data)