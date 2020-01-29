'''APIClient-driven tests'''
from dateutil.parser import isoparse
from rest_framework.reverse import reverse
from rest_framework import status
from rest_framework.test import APITestCase, APIClient
from balsam.server.serializers import UserSerializer
from balsam.server.models import User, Site

class TestAPIClient(APIClient):
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
            fn = self.parent.assertEquals
        else:
            fn = self.parent.assertIn
        return fn(response.status_code, expect_code, str(response.data))

    def post_data(self, view_name, uri=None, check=None, **kwargs):
        url = reverse(view_name, kwargs=uri)
        response = self.post(url, kwargs)
        self.check_stat(check, response)
        return response.data
    
    def put_data(self, view_name, uri=None, check=None, **kwargs):
        url = reverse(view_name, kwargs=uri)
        response = self.put(url, kwargs)
        self.check_stat(check, response)
        return response.data
    
    def patch_data(self, view_name, uri=None, check=None, **kwargs):
        url = reverse(view_name, kwargs=uri)
        response = self.patch(url, kwargs)
        self.check_stat(check, response)
        return response.data
    
    def get_data(self, view_name, uri=None, check=None, follow=False):
        url = reverse(view_name, kwargs=uri)
        response = self.get(url, follow=follow)
        self.check_stat(check, response)
        return response.data

class TestCase(APITestCase):
    @classmethod
    def setUpTestData(cls):
        """Called once per entire class! Don't modify users"""
        cls.user = User.objects.create_user(username='user', email='user@aol.com', password='abc')

    def setUp(self):
        """Called before each test"""
        self.client = TestAPIClient(self)
        self.client.login(username='user', password='abc')

class TwoUserTestCase(APITestCase):
    """Testing interactions from two clients"""
    @classmethod
    def setUpTestData(cls):
        """Called once per entire class! Don't modify users"""
        cls.user1 = User.objects.create_user(username='user1', email='user1@aol.com', password='abc')
        cls.user2 = User.objects.create_user(username='user2', email='user2@aol.com', password='123')

    def setUp(self):
        """Called before each test"""
        self.client1 = TestAPIClient(self)
        self.client1.login(username='user1', password='abc')
        self.client2 = TestAPIClient(self)
        self.client2.login(username='user2', password='123')

class AuthTests(TwoUserTestCase):
    def test_cannot_access_sites_after_logout(self):
        """One client logs out, then is forbidden from site-list. Does not affect other client"""
        self.client1.logout()
        dat = self.client1.get_data('site-list', check=status.HTTP_401_UNAUTHORIZED)
        dat = self.client2.get_data('site-list', check=status.HTTP_200_OK)
    
    def test_can_access_collections(self):
        """Can access all collections, except for User list"""
        self.client1.get_data('site-list', check=status.HTTP_200_OK)
        self.client1.get_data('user-list', check=status.HTTP_403_FORBIDDEN)
        self.client1.get_data('app-list', check=status.HTTP_200_OK)
        self.client1.get_data('batchjob-list', check=status.HTTP_200_OK)

    def test_api_root_shows_user_detail(self):
        user = self.client2.get_data('api-root', follow=True)
        self.assertEqual(user["pk"], self.user2.pk)

    def test_superuser_can_see_all_users(self):
        User.objects.create_user(username="super", password="abc", is_staff=True)
        staff_client = TestAPIClient(self)
        staff_client.login(username="super", password="abc")
        user_list = staff_client.get_data('user-list', check=status.HTTP_200_OK)
        self.assertEqual(len(user_list), 3)

class AppSharingTests(TwoUserTestCase):
    def test_no_shared_app(self):
        """client2 cannot see client1's apps by default"""
        site = self.client1.post_data('site-list', check=status.HTTP_201_CREATED, hostname='baz', path='/foo')
        app = self.client1.post_data(
            'app-list', check=status.HTTP_201_CREATED,
            name="hello world",
            backends=[{"site": site["pk"], "class_name": "Demo.SayHello"}],
            parameters=['name', 'N']
        )
        client1_apps = self.client1.get_data('app-list', check=status.HTTP_200_OK)
        self.assertEqual(len(client1_apps), 1)
        self.assertEqual(app, client1_apps[0])
        client2_apps = self.client2.get_data('app-list', check=status.HTTP_200_OK)
        self.assertEqual(len(client2_apps), 0)
    
    def test_shared_app(self):
        """If client1 shares his app with client2, then client2 can see it"""
        site = self.client1.post_data('site-list', check=status.HTTP_201_CREATED, hostname='baz', path='/foo')
        self.client1.post_data(
            'app-list', check=status.HTTP_201_CREATED,
            name="hello world",
            backends=[{"site": site["pk"], "class_name": "Demo.SayHello"}],
            parameters=['name', 'N'], users=[1,2]
        )
        client1_apps = self.client1.get_data('app-list', check=status.HTTP_200_OK)
        client2_apps = self.client2.get_data('app-list', check=status.HTTP_200_OK)
        self.assertEqual(client1_apps, client2_apps)

class SiteTests(TestCase):
    def create_site(self, hostname='baz', path='/foo', check=status.HTTP_201_CREATED):
        site_data = self.client.post_data(
            'site-list', hostname=hostname, path=path, check=check
        )
        return site_data

    def test_can_create_site(self):
        site = self.create_site()
        self.assertEqual(site["owner"], self.user.pk)
    
    def test_cannot_create_duplicate_site(self):
        self.create_site(hostname="theta", path="/projects/mysite1", check=status.HTTP_201_CREATED)
        self.create_site(hostname="theta", path="/projects/mysite1", check=status.HTTP_400_BAD_REQUEST)

    def test_created_site_in_list_view(self):
        site = self.create_site()
        site_list = self.client.get_data('site-list', check=status.HTTP_200_OK)
        self.assertEqual(site["hostname"], site_list[0]["hostname"])

    def test_detail_view(self):
        created_site = self.create_site()
        pk = created_site["pk"] 
        retrieved_site = self.client.get_data(
            'site-detail', uri={"pk":pk}, check=status.HTTP_200_OK
        )
        self.assertEqual(created_site, retrieved_site)

    def test_update_site_status(self):
        created_site = self.create_site()
        created_time = isoparse(created_site["last_refresh"])
        created_site["status"].update(dict(
            num_nodes=128,
            num_idle_nodes=10,
            num_busy_nodes=118,
            backfill_windows=[(8, 30), (2,120)],
        ))
        updated_site = self.client.put_data(
            'site-detail', uri={"pk":created_site["pk"]},
            check=status.HTTP_200_OK, **created_site
        )
        updated_time = isoparse(updated_site["last_refresh"])
        self.assertGreater(updated_time, created_time)
    
    def test_cannot_partial_update_owner(self):
        created_site = self.create_site()
        patch_dict = {"owner": 2}

        updated_site = self.client.patch_data(
            'site-detail', uri={"pk":created_site["pk"]},
            check=status.HTTP_200_OK, **patch_dict
        )
        self.assertEqual(updated_site["owner"], self.user.pk)
        self.assertNotEqual(updated_site["owner"], 2)
    
    def test_can_partial_update_status(self):
        # Create a hypothetical site with 118 busy nodes, 10 idle nodes
        site = self.create_site()
        site["status"].update(dict(
            num_nodes=128,
            num_idle_nodes=10,
            num_busy_nodes=118,
            backfill_windows=[(8, 30), (2,120)],
            queued_jobs=[
                {
                    "queue": "foo", "state": "queued", "num_nodes": 64, 
                    "score":120, "queued_time_min":32, "wall_time_min":60
                }, 
                {
                    "queue": "bar", "state": "running", "num_nodes": 54, 
                    "score":55, "queued_time_min":8, "wall_time_min":15
                }
            ]
        ))
        self.client.put_data(
            'site-detail', uri={"pk": site["pk"]},
            check=status.HTTP_200_OK, **site
        )

        # Patch: 8 nodes taken; now 2 idle & 126 busy
        patch_dict = dict(status={
            'backfill_windows': [(2,120)],
            'num_idle_nodes': 2,
            'num_busy_nodes': 126,
        })
        updated_site = self.client.patch_data(
            'site-detail', uri={"pk":site["pk"]},
            check=status.HTTP_200_OK, **patch_dict
        )
        site_status = updated_site["status"]
        self.assertEqual(site_status["num_nodes"], 128)
        self.assertEqual(site_status["num_idle_nodes"], 2)
        self.assertEqual(site_status["num_busy_nodes"], 126)
        self.assertEqual(site_status["num_down_nodes"], 0)
        self.assertEqual(site_status["backfill_windows"], [[2,120]])
        self.assertEqual(site_status["queued_jobs"], site["status"]["queued_jobs"])

    def test_delete_site(self):
        pass

class AppTests(TestCase):
    def test_can_create_app(self):
        pass

    def test_created_app_in_list_view(self):
        pass

    def test_created_app_appears_on_site_detail(self):
        pass

    def test_detail_view(self):
        pass

    def test_update_app(self):
        pass

    def test_delete_app(self):
        pass

    def test_app_merge(self):
        pass

class BatchJobTests(TestCase):
    def test_can_create_batchjob(self):
        pass

    def test_created_batchjob_in_list_view(self):
        pass

    def test_detail_view(self):
        pass

    def test_update_batchjob_before_running(self):
        pass
    
    def test_cannot_update_batchjob_after_running(self):
        pass
    
    def test_cannot_update_batchjob_after_terminal_state(self):
        pass
    
    def test_revert_stale_batchjob_update(self):
        pass

    def test_delete_batchjob(self):
        pass