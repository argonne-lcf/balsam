from rest_framework.test import APITestCase, APIClient
from rest_framework.reverse import reverse
from .util import pretty_data
from balsam.server.models import User


class BalsamAPIClient(APIClient):
    """Shortcut methods for get/post/etc that also test status code"""

    def __init__(self, test_case):
        self.parent = test_case
        super().__init__()

    def check_stat(self, expect_code, response):
        if expect_code is None:
            return
        try:
            iter(expect_code)
        except TypeError:
            fn = self.parent.assertEqual
        else:
            fn = self.parent.assertIn
        return fn(response.status_code, expect_code, pretty_data(response.data))

    def post_data(self, view_name, uri=None, check=None, **kwargs):
        url = reverse(view_name, kwargs=uri)
        response = self.post(url, kwargs)
        self.check_stat(check, response)
        return response.data

    def bulk_post_data(self, view_name, list_data, uri=None, check=None):
        url = reverse(view_name, kwargs=uri)
        response = self.post(url, list_data)
        self.check_stat(check, response)
        return response.data

    def put_data(self, view_name, uri=None, check=None, **kwargs):
        url = reverse(view_name, kwargs=uri)
        response = self.put(url, kwargs)
        self.check_stat(check, response)
        return response.data

    def bulk_put_data(self, view_name, list_data, uri=None, check=None):
        url = reverse(view_name, kwargs=uri)
        response = self.put(url, list_data)
        self.check_stat(check, response)
        return response.data

    def patch_data(self, view_name, uri=None, check=None, **kwargs):
        url = reverse(view_name, kwargs=uri)
        response = self.patch(url, kwargs)
        self.check_stat(check, response)
        return response.data

    def bulk_patch_data(self, view_name, list_data, uri=None, check=None):
        url = reverse(view_name, kwargs=uri)
        response = self.patch(url, list_data)
        self.check_stat(check, response)
        return response.data

    def delete_data(self, view_name, uri=None, check=None):
        url = reverse(view_name, kwargs=uri)
        response = self.delete(url)
        self.check_stat(check, response)
        return response.data

    def get_data(self, view_name, uri=None, check=None, follow=False, **kwargs):
        """GET kwargs become URL query parameters (e.g. /?site=3)"""
        url = reverse(view_name, kwargs=uri)
        response = self.get(url, data=kwargs, follow=follow)
        self.check_stat(check, response)
        return response.data


class TestCase(APITestCase):
    maxDiff = None

    @classmethod
    def setUpTestData(cls):
        """Called once per entire class! Don't modify users"""
        cls.user = User.objects.create_user(
            username="user", email="user@aol.com", password="abc"
        )

    def setUp(self):
        """Called before each test"""
        self.client = BalsamAPIClient(self)
        self.client.login(username="user", password="abc")

    def assertEqual(self, first, second, msg=None):
        if msg is not None and not isinstance(msg, str):
            msg = pretty_data(msg)
        return super().assertEqual(first, second, msg=msg)

    def assertIn(self, member, container, msg=None):
        if msg is not None and not isinstance(msg, str):
            msg = pretty_data(msg)
        return super().assertIn(member, container, msg=msg)


class TwoUserTestCase(APITestCase):
    """Testing interactions from two clients"""

    maxDiff = None

    @classmethod
    def setUpTestData(cls):
        """Called once per entire class! Don't modify users"""
        cls.user1 = User.objects.create_user(
            username="user1", email="user1@aol.com", password="abc"
        )
        cls.user2 = User.objects.create_user(
            username="user2", email="user2@aol.com", password="123"
        )

    def setUp(self):
        """Called before each test"""
        self.client1 = BalsamAPIClient(self)
        self.client1.login(username="user1", password="abc")
        self.client2 = BalsamAPIClient(self)
        self.client2.login(username="user2", password="123")
