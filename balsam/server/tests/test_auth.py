from balsam.server.models import User
from rest_framework import status
from .util import QueryProfiler
from .clients import (
    BalsamAPIClient,
    TwoUserTestCase,
)


class AuthTests(TwoUserTestCase):
    def test_cannot_access_sites_after_logout(self):
        """One client logs out, then is forbidden from site-list. Does not affect other client"""
        self.client1.logout()
        with QueryProfiler("site list: unauthorized"):
            self.client1.get_data("site-list", check=status.HTTP_401_UNAUTHORIZED)
        with QueryProfiler("site list: authorized"):
            self.client2.get_data("site-list", check=status.HTTP_200_OK)

    def test_can_access_collections(self):
        """Can access all collections, except for User list"""
        self.client1.get_data("site-list", check=status.HTTP_200_OK)
        self.client1.get_data("user-list", check=status.HTTP_403_FORBIDDEN)
        self.client1.get_data("app-list", check=status.HTTP_200_OK)
        self.client1.get_data("batchjob-list", check=status.HTTP_200_OK)

    def test_api_root_shows_user_detail(self):
        user = self.client2.get_data("api-root", follow=True)
        self.assertEqual(user["pk"], self.user2.pk)

    def test_superuser_can_see_all_users(self):
        User.objects.create_user(username="super", password="abc", is_staff=True)
        staff_client = BalsamAPIClient(self)
        staff_client.login(username="super", password="abc")
        user_list = staff_client.get_data("user-list", check=status.HTTP_200_OK)
        self.assertEqual(user_list["count"], 3)
