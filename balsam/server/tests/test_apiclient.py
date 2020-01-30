'''APIClient-driven tests'''
from dateutil.parser import isoparse
from rest_framework.reverse import reverse
from rest_framework import status
from rest_framework.test import APITestCase, APIClient
from balsam.server.serializers import UserSerializer
from balsam.server.models import User, Site, AppBackend, AppExchange

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
    
    def delete_data(self, view_name, uri=None, check=None):
        url = reverse(view_name, kwargs=uri)
        response = self.delete(url)
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

class SiteFactoryMixin:
    def create_site(self, hostname='baz', path='/foo', check=status.HTTP_201_CREATED):
        site_data = self.client.post_data(
            'site-list', hostname=hostname, path=path, check=check
        )
        return site_data
    
class AppFactoryMixin:
    def create_app(self, name="hello world", backends=None, parameters=None, check=status.HTTP_201_CREATED):
        backends = [{"site": pk, "class_name": name} for pk,name in backends]
        if parameters is None:
            parameters = ['name', 'N']
        app_data = self.client.post_data(
            'app-list', check=check,
            name=name,
            backends=backends,
            parameters=parameters,
        )
        return app_data

class SiteTests(TestCase, SiteFactoryMixin):

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
            backfill_windows=[[8, 30], [2,120]],
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
            'backfill_windows': [[2,120]],
            'num_idle_nodes': 2,
            'num_busy_nodes': 126,
        })
        target_site = site.copy()
        target_site["status"].update(**patch_dict["status"])

        updated_site = self.client.patch_data(
            'site-detail', uri={"pk":site["pk"]},
            check=status.HTTP_200_OK, **patch_dict
        )

        # The patch was successful: updated_site is identical to expected
        # barring the "last_refresh" time stamp
        updated_site.pop('last_refresh')
        target_site.pop('last_refresh')
        self.assertEqual(updated_site, target_site)

    def test_deleting_site_removes_associated_backends(self):
        site = self.create_site()
        # Register a new app with one backend at this site
        app = self.client.post_data(
            'app-list', check=status.HTTP_201_CREATED,
            name="hello world",
            backends=[{"site": site["pk"], "class_name": "Demo.SayHello"}],
            parameters=['name', 'N']
        )
        backends = app["backends"]
        self.assertEqual(len(backends), 1)
        self.assertEqual(backends[0]['site'], site['pk'])

        # Now delete the site.  The app should remain, but with 0 backends.
        self.client.delete_data(
            'site-detail', uri={"pk":site["pk"]}, check=status.HTTP_204_NO_CONTENT
        )
        app = self.client.get_data('app-detail', uri={"pk":app["pk"]}, check=status.HTTP_200_OK)
        self.assertEqual(len(app["backends"]), 0)

class AppTests(TestCase, SiteFactoryMixin, AppFactoryMixin):
    def test_can_create_app(self):
        site = self.create_site()
        app = self.create_app(backends=[(site["pk"], 'DemoApp.hello')])
        self.assertEqual(app["backends"][0]["class_name"], 'DemoApp.hello')

    def test_created_app_in_list_view(self):
        site = self.create_site()
        app = self.create_app(backends=[(site["pk"], 'DemoApp.hello')])
        self.assertEqual(app['backends'][0]['site'], site['pk'])

        # Retrieve the app list; ensure the App shows up 
        app_list = self.client.get_data('app-list', check=status.HTTP_200_OK)
        self.assertEqual(len(app_list), 1)
        self.assertEqual(app_list[0], app)

    def test_created_app_appears_on_site_detail(self):
        site = self.create_site()
        app = self.create_app(backends=[(site["pk"], 'Foo.bar')])
        app_retrieved = self.client.get_data(
            'app-detail', uri={'pk':app["pk"]}
        )
        self.assertEqual(app, app_retrieved)
        backend = app["backends"][0]
        self.assertEqual(backend["site"], site["pk"])
        self.assertEqual(backend["class_name"], 'Foo.bar')

    def test_cannot_create_duplicate_name(self):
        site1 = self.create_site()
        site2 = self.create_site(hostname="otherhost")
        app1 = self.create_app(name="foo12", backends=[(site1["pk"], 'Foo.bar')],check=status.HTTP_201_CREATED)
        app2 = self.create_app(name="foo12", backends=[(site2["pk"], 'Foo.bar')],check=status.HTTP_400_BAD_REQUEST)

    def test_update_app_backends(self):
        site1 = self.create_site(hostname="a", path='/my/Project1')
        site2 = self.create_site(hostname="a", path='/my/Project2')
        site3 = self.create_site(hostname="b", path='/foo/bar')
        old_app = self.create_app(backends=[(site1["pk"], 'Simulations.calcX')])

        # Patch app to have 3 new backends
        backends_patch = [
            {"site": site1["pk"], "class_name": "renamed_simulation.calc"},
            {"site": site2["pk"], "class_name": "simulation.calc"},
            {"site": site3["pk"], "class_name": "simulation.calc"},
        ]
        app = self.client.patch_data(
            'app-detail', uri={"pk": old_app["pk"]},
            backends=backends_patch, check=status.HTTP_200_OK
        )
        # The new backends match the intended patch (as far as site & class_name)
        new_backends = app.pop('backends')
        new_backends = [{"site":d["site"], "class_name":d["class_name"]} for d in new_backends]
        self.assertEqual(backends_patch, new_backends)
        # Otherwise, the app is unchanged
        old_app.pop('backends')
        self.assertEqual(old_app, app)

    def test_delete_app(self):
        site1 = self.create_site(hostname="a", path='/my/Project1')
        site2 = self.create_site(hostname="a", path='/my/Project2')
        app_local = self.create_app(
            name="foo_local",  
            backends=[(site1["pk"], 'Foo.bar')], 
            check=status.HTTP_201_CREATED
        )
        app_shared = self.create_app(
            name="foo_dualsite", 
            backends=[(site1["pk"], 'Foo.bar'), (site2["pk"], 'Foo.bar')],
            check=status.HTTP_201_CREATED
        )
        # Peek into DB: there are only 2 backends
        self.assertEqual(AppBackend.objects.count(), 2)
        # Now the dual-backend app is deleted, leaving only the first backend
        self.client.delete_data(
            'app-detail', uri={'pk': app_shared["pk"]},
            check=status.HTTP_204_NO_CONTENT
        )
        self.assertEqual(AppBackend.objects.count(), 1)
        sites = self.client.get_data('site-list')
        sites = {s["pk"]: s for s in sites}
        self.assertEqual(sites[site1["pk"]]["apps"], ['Foo.bar'])
        self.assertEqual(sites[site2["pk"]]["apps"], [])

    def test_app_merge(self):
        site1 = self.create_site(hostname="theta", path='/my/Project1')
        site2 = self.create_site(hostname="cooley", path='/my/Project2')
        app1 = self.create_app(
            name="foo_theta",  
            backends=[(site1["pk"], 'Foo.bar')], 
            check=status.HTTP_201_CREATED
        )
        app2 = self.create_app(
            name="foo_cooley",  
            backends=[(site2["pk"], 'Foo.bar')], 
            check=status.HTTP_201_CREATED
        )
        app3 = self.client.post_data(
            'app-merge',
            name="foo_merged",
            description="",
            existing_apps=[ app1["pk"], app2["pk"] ],
            check=status.HTTP_201_CREATED
        )
        self.assertEqual(app1["parameters"], app3["parameters"])
        self.assertEqual(len(app3["backends"]), 2)
        
class BatchJobTests(TestCase):

    def test_can_create_batchjob(self):
        self.client.post_data(
            'batchjob-list',
        )

    def test_list_batchjobs_spanning_sites(self):
        pass

    def test_bulk_status_update_batch_jobs(self):
        pass

    def test_json_tags_filter_list(self):
        pass
    
    def test_filter_by_site(self):
        pass
    
    def test_filter_by_time_range(self):
        pass

    def test_paginated_responses(self):
        pass

    def test_search_by_hostname(self):
        pass

    def test_order_by_listings(self):
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