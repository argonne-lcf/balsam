'''APIClient-driven tests'''
import pprint
from datetime import datetime, timedelta
from dateutil.parser import isoparse
import random
from rest_framework.reverse import reverse
from rest_framework import status
from rest_framework.test import APITestCase, APIClient
from balsam.server.serializers import UserSerializer
from balsam.server.models import User, Site, AppBackend, AppExchange, BatchJob


def pretty_data(data):
    return '\n' + pprint.pformat(data, width=93, indent=2)

class SiteFactoryMixin:
    def create_site(self, hostname='baz', path='/foo', check=status.HTTP_201_CREATED):
        return self.client.post_data('site-list', hostname=hostname, path=path, check=check)
    
class AppFactoryMixin:
    def create_app(self, name="hello world", sites=None, cls_names=None, parameters=None, check=status.HTTP_201_CREATED):
        """Sites: dict with pk, or list of dicts with pk, or list of ints, or int"""
        # Site validation
        if isinstance(sites, dict): sites = [int(sites["pk"])]
        elif isinstance(sites, list): sites = [(int(s["pk"]) if isinstance(s, dict) else int(s)) for s in sites]
        elif isinstance(sites, int): sites = [sites]
        else: raise ValueError("sites must be a single-or-list of ints or dicts-with-pk")
        # Class Names validation
        if isinstance(cls_names, str): cls_names = [cls_names]
        elif isinstance(cls_names, list): cls_names = [str(s) for s in cls_names]
        else: raise ValueError('cls_names')

        backends = [{"site": pk, "class_name": name} for pk,name in zip(sites, cls_names)]
        if parameters is None: parameters = ['name', 'N']
        return self.client.post_data(
            'app-list', check=check, name=name,
            backends=backends, parameters=parameters,
        )

class BatchJobFactoryMixin:
    def create_batchjob(
        self, site, project='datascience', queue='default', num_nodes=128, wall_time_min=30,
        job_mode='mpi', filter_tags={"system": 'H2O', "type": 'sp_energy'}, check=status.HTTP_201_CREATED
    ):
        return self.client.post_data(
            'batchjob-list', site=site['pk'], project=project, queue=queue, num_nodes=num_nodes,
            wall_time_min=wall_time_min, job_mode=job_mode, filter_tags=filter_tags, check=check
        )


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
        cls.user = User.objects.create_user(username='user', email='user@aol.com', password='abc')

    def setUp(self):
        """Called before each test"""
        self.client = TestAPIClient(self)
        self.client.login(username='user', password='abc')

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
        self.assertDictEqual(app, client1_apps[0])
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
        self.assertListEqual(client1_apps, client2_apps)

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
        self.assertDictEqual(created_site, retrieved_site)

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
        self.assertDictEqual(updated_site, target_site)

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
        app = self.create_app(sites=site, cls_names='DemoApp.hello')
        self.assertEqual(app["backends"][0]["class_name"], 'DemoApp.hello')

    def test_created_app_in_list_view(self):
        site = self.create_site()
        app = self.create_app(sites=site, cls_names='DemoApp.hello')
        self.assertEqual(app['backends'][0]['site'], site['pk'])

        # Retrieve the app list; ensure the App shows up 
        app_list = self.client.get_data('app-list', check=status.HTTP_200_OK)
        self.assertEqual(len(app_list), 1)
        self.assertDictEqual(app_list[0], app)

    def test_created_app_appears_on_site_detail(self):
        site = self.create_site()
        app = self.create_app(sites=site, cls_names='Foo.bar')
        app_retrieved = self.client.get_data(
            'app-detail', uri={'pk':app["pk"]}
        )
        self.assertDictEqual(app, app_retrieved)
        backend = app["backends"][0]
        self.assertEqual(backend["site"], site["pk"])
        self.assertEqual(backend["class_name"], 'Foo.bar')

    def test_cannot_create_duplicate_name(self):
        site1 = self.create_site()
        site2 = self.create_site(hostname="otherhost")
        app1 = self.create_app(name="foo12", sites=site1, cls_names='Foo.bar', check=status.HTTP_201_CREATED)
        app2 = self.create_app(name="foo12", sites=site2, cls_names='Foo.bar', check=status.HTTP_400_BAD_REQUEST)

    def test_update_app_backends(self):
        site1 = self.create_site(hostname="a", path='/my/Project1')
        site2 = self.create_site(hostname="a", path='/my/Project2')
        site3 = self.create_site(hostname="b", path='/foo/bar')
        old_app = self.create_app(sites=site1, cls_names='Simulations.calcX')

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
        self.assertListEqual(backends_patch, new_backends)
        # Otherwise, the app is unchanged
        old_app.pop('backends')
        self.assertDictEqual(old_app, app)

    def test_delete_app(self):
        site1 = self.create_site(hostname="a", path='/my/Project1')
        site2 = self.create_site(hostname="a", path='/my/Project2')
        app_local = self.create_app(name="foo_local", sites=site1, cls_names='Foo.bar', check=status.HTTP_201_CREATED)
        app_shared = self.create_app(
            name="foo_dualsite", 
            sites=[site1, site2], cls_names=['Foo.bar', 'Foo.bar'],
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
            name="foo_theta", sites=site1, cls_names='Foo.bar', 
            check=status.HTTP_201_CREATED
        )
        app2 = self.create_app(
            name="foo_cooley", sites=site2, cls_names='Foo.bar',
            check=status.HTTP_201_CREATED
        )
        app3 = self.client.post_data(
            'app-merge', name="foo_merged",
            existing_apps=[ app1["pk"], app2["pk"] ],
            check=status.HTTP_201_CREATED
        )
        self.assertEqual(app1["parameters"], app3["parameters"])
        self.assertEqual(len(app3["backends"]), 2)
        
class BatchJobTests(TestCase, SiteFactoryMixin, BatchJobFactoryMixin):

    def test_can_create_batchjob(self):
        site = self.create_site()
        batch_job = self.create_batchjob(site=site, check=status.HTTP_201_CREATED)
        self.assertIn("status_message", batch_job)
        self.assertIn("scheduler_id", batch_job)
        self.assertEqual(batch_job["state"], "pending-submission", msg=batch_job)

    def test_list_batchjobs_spanning_sites(self):
        site1 = self.create_site(hostname="1")
        site2 = self.create_site(hostname="2")
        for time in [10,20,30,40]:
            for site in [site1, site2]:
                self.create_batchjob(site=site, wall_time_min=time)
        bjob_list = self.client.get_data('batchjob-list')
        self.assertEqual(bjob_list["count"], 8)
        self.assertEqual(len(bjob_list["results"]), 8)

    def test_filter_by_site(self):
        site1 = self.create_site(hostname="1")
        site2 = self.create_site(hostname="2")
        for time in [10,20,30,40]:
            for site in [site1, site2]:
                self.create_batchjob(site=site, wall_time_min=time)

        # providing GET kwargs causes result list to be filtered
        dat = self.client.get_data('batchjob-list', site=site2["pk"])
        self.assertEqual(dat["count"], 4)
        results = dat["results"]
        self.assertEqual(len(results), 4)
        self.assertListEqual([j["site"] for j in results], 4*[site2["pk"]])
    
    def test_filter_by_time_range(self):
        site = self.create_site()
        # Create 10 historical batchjobs
        # Job n started n hours ago and took 30 minutes
        pks = []
        now = datetime.utcnow() # IMPORTANT! all times in UTC
        for i in range(1, 11):
            j = self.create_batchjob(site=site, job_mode="serial")
            start = now - timedelta(hours=i*1)
            end = start + timedelta(minutes=30)
            j.update(state='finished', start_time=start, end_time=end)
            if now-timedelta(hours=5) <= end <= now-timedelta(hours=3):
                j["filter_tags"].update(good="Yes")
            self.client.put_data(
                'batchjob-detail', uri={'pk': j["pk"]}, **j,
                check=status.HTTP_200_OK
            )

        # Now, we want to filter for jobs that ended between 3 and 5 hours ago
        # The end_times are: 0.5h ago, 1.5 ago, 2.5, 3.5, 4.5, 5.5, ...
        # So we should have 2 jobs land in this window
        end_after=(now-timedelta(hours=5)).isoformat()+'Z'
        end_before=(now-timedelta(hours=3)).isoformat()+'Z'
        jobs = self.client.get_data(
            'batchjob-list',
            end_time_after=end_after,
            end_time_before=end_before,
            check=status.HTTP_200_OK
        )
        self.assertEqual(jobs["count"], 2)
        jobs = jobs["results"]
        for job in jobs:
            self.assertIn("good", job["filter_tags"])
    
    def test_json_tags_filter_list(self):
        site = self.create_site()
        for priority in [None, 1, 2, 3]:
            for system in ["H2O", "D2O", "HF"]:
                if priority: tags = {"priority": priority, "system": system}
                else: tags = {"system": system}
                self.create_batchjob(site, filter_tags=tags)

        jobs = self.client.get_data('batchjob-list')
        self.assertEqual(jobs['count'], 12)
        jobs = self.client.get_data('batchjob-list', filter_tags__priority__gt=1)
        self.assertEqual(jobs['count'], 6)
        jobs = self.client.get_data('batchjob-list', filter_tags__priority__lt=1)
        self.assertEqual(jobs['count'], 0)
        jobs = self.client.get_data('batchjob-list', filter_tags__priority=3)
        self.assertEqual(jobs['count'], 3)
        jobs = self.client.get_data('batchjob-list', filter_tags__priority__isnull=True)
        self.assertEqual(jobs['count'], 3)
        
        jobs = self.client.get_data('batchjob-list', filter_tags__system='D2O')
        self.assertEqual(jobs['count'], 4)
        
        jobs = self.client.get_data(
            'batchjob-list', 
            filter_tags__system__icontains='F',
            filter_tags__priority__isnull=True
        )
        self.assertEqual(jobs['count'], 1)
        
    def test_search_by_hostname(self):
        site1 = self.create_site(hostname='theta')
        site2 = self.create_site(hostname='cooley')
        for s in [site1, site2]:
            for num_nodes in [128,256]:
                self.create_batchjob(site=s, num_nodes=num_nodes)
        jobs = self.client.get_data('batchjob-list', search="thet")
        self.assertEqual(jobs['count'], 2)
        self.assertEqual(jobs['results'][0]['site'], site1['pk'])
        self.assertEqual(jobs['results'][1]['site'], site1['pk'])

    def test_order_by_listings(self):
        # Create a shuffled list of batchjobs
        site = self.create_site(hostname='theta')
        states = 5*['finished'] + 5*['running']
        deltas = [timedelta(hours=random.randint(-30,-1)) for i in range(10)]
        random.shuffle(states)
        now = datetime.utcnow()
        start_times = [now + delta for delta in deltas]

        for state, start in zip(states, start_times):
            j = self.create_batchjob(site)
            j.update(state=state, start_time=start)
            self.client.put_data(
                'batchjob-detail', uri={"pk": j["pk"]},
                check=status.HTTP_200_OK, **j
            )

        # Order by state and descending start time
        jobs = self.client.get_data(
            'batchjob-list', ordering="state,-start_time",
            check=status.HTTP_200_OK
        )
        jobs = jobs['results']
        self.assertEqual(len(jobs), 10)
        states = [j["state"] for j in jobs]
        stimes_finished = [isoparse(j["start_time"]) for j in jobs[:5]]
        stimes_running = [isoparse(j["start_time"]) for j in jobs[5:]]
        self.assertListEqual(states, sorted(states))
        self.assertListEqual(stimes_finished, sorted(stimes_finished, reverse=True))
        self.assertListEqual(stimes_running, sorted(stimes_running, reverse=True))

    def test_paginated_responses(self):
        site = self.create_site(hostname='theta')
        site = Site.objects.first()
        jobs = [BatchJob(site=site,num_nodes=1,wall_time_min=1,job_mode='mpi') for i in range(2000)]
        BatchJob.objects.bulk_create(jobs)

        # default page size is 100
        jobs = self.client.get_data('batchjob-list')
        self.assertEqual(jobs['count'], 2000)
        self.assertEqual(len(jobs['results']), 100)
        self.assertIn('limit', jobs["next"])
        self.assertEqual(None, jobs["previous"]) # no previous page

        # larger page and offset:
        jobs = self.client.get_data('batchjob-list',limit=800,offset=200)
        self.assertEqual(jobs['count'], 2000)
        self.assertEqual(len(jobs['results']), 800)
        self.assertIn('limit', jobs["next"])
        self.assertIn('limit', jobs["previous"])
    
    def test_detail_view(self):
        site = self.create_site(hostname='theta')
        job = self.create_batchjob(site)
        self.client.get_data('batchjob-detail', uri={'pk': job['pk']}, check=status.HTTP_200_OK)
    
    def test_update_to_invalid_state(self):
        site = self.create_site(hostname='theta')
        job = self.create_batchjob(site)
        job.update(num_nodes=4096, state='invalid-state')
        self.client.put_data('batchjob-detail', uri={'pk': job['pk']}, **job, check=status.HTTP_400_BAD_REQUEST)
    
    def test_update_valid(self):
        site = self.create_site(hostname='theta')
        job = self.create_batchjob(site)
        job.update(status_message="Please submit to another queue", state='submit-failed')
        self.client.put_data('batchjob-detail', uri={'pk': job['pk']}, **job, check=status.HTTP_200_OK)
        ret = self.client.get_data('batchjob-detail', uri={'pk': job['pk']})
        self.assertDictEqual(ret, job)
    
    def test_partial_update(self):
        site = self.create_site(hostname='theta')
        job = self.create_batchjob(site)
        patch = dict(status_message="You dont have permission to submit to this queue", state='submit-failed')
        patched_job = self.client.patch_data('batchjob-detail', uri={'pk': job['pk']}, **patch, check=status.HTTP_200_OK)
        expected_result = job.copy()
        expected_result.update(patch)
        self.assertDictEqual(patched_job, expected_result)

    def test_bulk_status_update_batch_jobs(self):
        theta = self.create_site(hostname='theta')
        cooley = self.create_site(hostname='cooley')
        for _ in range(10):
            self.create_batchjob(theta)
            self.create_batchjob(cooley)

        # scheduler agent receives 10 batchjobs; sends back bulk-state updates
        jobs = self.client.get_data('batchjob-list', site=cooley["pk"])
        self.assertEqual(jobs['count'], 10)
        jobs = jobs['results']
        for job in jobs[:5]:
            job["state"] = "queued"
        for job in jobs[5:]:
            job["state"] = "running"
            job["start_time"] = datetime.utcnow() + timedelta(minutes=random.randint(-30,0))

        updates = [{k:job[k] for k in job if k in ['pk', 'state', 'start_time']} for job in jobs]
        result = self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=updates
        )

        for updated_job in result:
            pk = updated_job['pk']
            expected_job = next(j for j in jobs if j['pk']==pk)
            if expected_job['start_time'] is not None:
                expected_job['start_time'] = expected_job['start_time'].isoformat() + 'Z'
            self.assertDictEqual(updated_job, expected_job)
        
        jobs = self.client.get_data(
            'batchjob-list', site=cooley["pk"], state='running'
        )
        self.assertEqual(jobs['count'], 5)

    def test_update_batchjob_before_running(self):
        site = self.create_site(hostname='theta')
        pk = self.create_batchjob(site, filter_tags={'system': 'H2O'})['pk']

        # The Balsam site (agent) and user retrieve job at same time
        user_job = self.client.get_data('batchjob-detail', uri={'pk':pk})
        site_job = self.client.get_data('batchjob-detail', uri={'pk': pk})

        # first the Site submits the job and bulk-partial-updates as "queued"
        site_job["state"] = 'queued'
        site_job["scheduler_id"] = 123
        self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=[{'pk':pk, 'state': 'queued', 'scheduler_id': 123}]
        )

        # Meanwhile, another client clears-out filter_tags on their stale job
        # We need to be using PATCH and partial-updating, to reduce likelihood
        # of clobbering updates with stale data
        user_job['filter_tags'] = {}
        user_job = self.client.patch_data(
            'batchjob-detail', uri={'pk':pk}, filter_tags={},
            check=status.HTTP_200_OK
        )

        self.assertEqual(user_job['filter_tags'], {})
        self.assertEqual(user_job['state'], 'queued')
        self.assertEqual(user_job['scheduler_id'], 123)
    
    def test_cannot_update_batchjob_after_running(self):
        site = self.create_site(hostname='theta')
        pk = self.create_batchjob(site, num_nodes=7)['pk']
        # The Balsam site (agent) and user retrieve job at same time
        user_job = self.client.get_data('batchjob-detail', uri={'pk':pk})
        site_job = self.client.get_data('batchjob-detail', uri={'pk': pk})

        # Site runs job first
        self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=[{'pk':pk, 'state': 'running', 'scheduler_id': 123}]
        )

        # Client attempts to change num_nodes with stale record
        response = self.client.patch_data(
            'batchjob-detail', uri={'pk':pk}, num_nodes=27,
            check=status.HTTP_400_BAD_REQUEST
        )
        self.assertIn('cannot be updated', response[0])

    def test_revert_stale_batchjob_update(self):
        # A BatchJob is added to user's theta site.
        site = self.create_site(hostname='theta')
        pk = self.create_batchjob(site, num_nodes=7)['pk']
        
        # The balsam site retrieves the new job
        site_job = self.client.get_data('batchjob-detail', uri={'pk': pk})
        self.assertEqual(site_job['state'], 'pending-submission')

        # The site submits the job to the local queue

        # The site updates state as "queued". Now there is a scheduler_id.
        site_job = self.client.patch_data(
            'batchjob-detail', uri={'pk': pk}, state='queued',
            scheduler_id=123, check=status.HTTP_200_OK
        )

        # Time goes by..the job has started running

        # The Balsam site (agent) and web client retrieve BatchJob
        site_job = self.client.get_data('batchjob-detail', uri={'pk': pk})
        web_user_job = self.client.get_data('batchjob-detail', uri={'pk':pk})

        # Web Client doesnt know it's running. Updates num_nodes=27
        web_user_job = self.client.patch_data(
            'batchjob-detail', uri={'pk':pk}, num_nodes=27,
            check=status.HTTP_200_OK
        )
        self.assertEqual(web_user_job['num_nodes'], 27)

        # Balsam site does qstat: the job has started running on 7 nodes
        qstat = {'state': 'running', 'scheduler_id': 123, 'num_nodes': 7}

        # The site's record is stale!
        self.assertEqual(site_job['num_nodes'], 7)
        self.assertEqual(BatchJob.objects.get(pk=pk).num_nodes, 27)
        
        # In order to mitigate these invalid updates on stale BatchJobs, the site
        # includes revert=True on all 'running' status updates
        site_job.update(**qstat, revert=True)
        self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=[site_job]
        )

        # The BatchJob record is now updated to running and all 
        # fields forced to match qstat values
        web_user_job = self.client.get_data('batchjob-detail', uri={'pk':pk})
        site_job.pop('revert')
        self.assertDictEqual(web_user_job, site_job)
        self.assertEqual(site_job['num_nodes'], 7)
        self.assertEqual(site_job['state'], 'running')
    
    def test_revert_does_not_override_deletion_state(self):
        site = self.create_site(hostname='theta')
        pk = self.create_batchjob(site, num_nodes=7)['pk']

        user_job = self.client.get_data('batchjob-detail', uri={'pk':pk})
        site_job = self.client.get_data('batchjob-detail', uri={'pk':pk})

        # One client marks job for deletion
        user_job = self.client.patch_data(
            'batchjob-detail', uri={'pk':pk}, check=status.HTTP_200_OK,
            state='pending-deletion'
        )

        # Site updates job for running (therefore revert=True to enforce consistency)
        site_job.update(scheduler_id=444, state='running', revert=True)
        site_job = self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=[site_job]
        )[0]

        # However, state "running" does not overwrite "pending-deletion"
        self.assertEqual(site_job['state'], 'pending-deletion')
        self.assertEqual(site_job['scheduler_id'], 444)
        # Now the site knows to 'qdel' the job...
        site_job.update(state='finished')
        site_job = self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=[site_job]
        )[0]
        self.assertEqual(site_job['state'], 'finished')

    def test_cannot_update_batchjob_after_terminal_state(self):
        site = self.create_site(hostname='theta')
        pk = self.create_batchjob(site, num_nodes=7)['pk']
        user_job = self.client.get_data('batchjob-detail', uri={'pk':pk})
        site_job = self.client.get_data('batchjob-detail', uri={'pk':pk})

        site_job.update(state='finished')
        site_job = self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=[site_job]
        )[0]

        user_job = self.client.patch_data(
            'batchjob-detail', uri={'pk':pk}, state='pending-deletion',
            check=status.HTTP_400_BAD_REQUEST
        )
        self.assertIn('state can no longer change', user_job[0])

    def test_delete_running_batchjob(self):
        site = self.create_site(hostname='theta')
        pk = self.create_batchjob(site, num_nodes=7)['pk']
        user_job = self.client.get_data('batchjob-detail', uri={'pk':pk})
        site_job = self.client.get_data('batchjob-detail', uri={'pk':pk})
        self.assertEqual(user_job['scheduler_id'], None)

        # site updates to running
        site_job.update(
            state='running', start_time=datetime.utcnow(), scheduler_id=123,
            revert=True
        )
        site_job = self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=[site_job]
        )[0]
        self.assertEqual(site_job['state'], 'running')

        # user patches to pending-deletion
        user_job = self.client.patch_data(
            'batchjob-detail', uri={'pk':pk}, check=status.HTTP_200_OK,
            state='pending-deletion'
        )
        self.assertEqual(user_job['state'], 'pending-deletion')
        self.assertEqual(user_job['scheduler_id'], 123)
        
        # Client receives job marked for deletion
        site_job = self.client.get_data('batchjob-detail', uri={'pk':pk})
        self.assertEqual(site_job['state'], 'pending-deletion')
        site_job.update(state='finished', status_message='user-deleted', end_time=datetime.utcnow())
        patch = {k: site_job[k] for k in ['pk', 'state', 'status_message', 'end_time']}
        site_job = self.client.bulk_patch_data(
            'batchjob-list', check=status.HTTP_200_OK,
            list_data=[patch]
        )[0]
        self.assertEqual(site_job['state'], 'finished')