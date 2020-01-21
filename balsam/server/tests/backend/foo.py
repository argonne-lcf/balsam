from rest_framework.reverse import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from balsam.models import User

class AuthTests(APITestCase):

    MOCK_PASSWORD="abc123" 

    def create_user(self, name):
        user = User.objects.create_user(username=name, password=self.MOCK_PASSWORD)
        return user

    def login_session(self, user):
        self.client.login(username=user.username, password=self.MOCK_PASSWORD)

    def setUp(self):
        user = self.create_user('Bob')
        self.login_session(user)

    def test_unauthorized_user_cannot_access_apps(self):
        """Log out, then attempt app-list. Must be forbidden."""
        self.client.logout()
        url = reverse('app-list')
        response = self.client.get(url)
        self.assertIn(response.status_code, [status.HTTP_403_FORBIDDEN, status.HTTP_401_UNAUTHORIZED])
    
    def test_authorized_user_can_see_self(self):
        """Logged-in user can list apps"""
        url = reverse('app-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_add_site(self):
        """User can add site"""
        # Register a site: get back token OK
        url = reverse('site-register')
        resp = self.client.post(url, data={})
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        token = resp.json()['token']
        
        # Site is not yet visible in list
        url = reverse('site-list')
        sites = self.client.get(url).json()
        self.assertEqual(sites, [])

        # Activate by POSTing host, site_path
        site_dat = dict(hostname="myhost", site_path="/projects/foo")
        url = reverse('site-activate')
        resp = self.client.post(url, data=site_dat, HTTP_AUTHORIZATION=f'Token {token}')
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        site = resp.json()
        self.assertEqual(site["site_path"], "/projects/foo")

        # Re-activating gives error
        resp = self.client.post(url, data=site_dat, HTTP_AUTHORIZATION=f'Token {token}')
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn('already activated', resp.json()['errors'])

        # Site is now visible in list
        url = reverse('site-list')
        sites = self.client.get(url).json()
        self.assertEqual(len(sites), 1)
        site = sites[0]
        self.assertEqual(site['hostname'], 'myhost')