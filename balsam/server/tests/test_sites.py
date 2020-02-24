from rest_framework import status
from dateutil.parser import isoparse
from .clients import TestCase
from .mixins import SiteFactoryMixin


class SiteTests(TestCase, SiteFactoryMixin):
    def test_can_create_site(self):
        site = self.create_site()
        self.assertEqual(site["owner"], self.user.pk)

    def test_cannot_create_duplicate_site(self):
        self.create_site(
            hostname="theta", path="/projects/mysite1", check=status.HTTP_201_CREATED
        )
        self.create_site(
            hostname="theta",
            path="/projects/mysite1",
            check=status.HTTP_400_BAD_REQUEST,
        )

    def test_created_site_in_list_view(self):
        site = self.create_site()
        site_list = self.client.get_data("site-list", check=status.HTTP_200_OK)
        self.assertEqual(site["hostname"], site_list["results"][0]["hostname"])

    def test_detail_view(self):
        created_site = self.create_site()
        pk = created_site["pk"]
        retrieved_site = self.client.get_data(
            "site-detail", uri={"pk": pk}, check=status.HTTP_200_OK
        )
        self.assertDictEqual(created_site, retrieved_site)

    def test_update_site_status(self):
        created_site = self.create_site()
        created_time = isoparse(created_site["last_refresh"])
        created_site["status"].update(
            dict(
                num_nodes=128,
                num_idle_nodes=10,
                num_busy_nodes=118,
                backfill_windows=[(8, 30), (2, 120)],
            )
        )
        updated_site = self.client.put_data(
            "site-detail",
            uri={"pk": created_site["pk"]},
            check=status.HTTP_200_OK,
            **created_site,
        )
        updated_time = isoparse(updated_site["last_refresh"])
        self.assertGreater(updated_time, created_time)

    def test_cannot_partial_update_owner(self):
        created_site = self.create_site()
        patch_dict = {"owner": 2}

        updated_site = self.client.patch_data(
            "site-detail",
            uri={"pk": created_site["pk"]},
            check=status.HTTP_200_OK,
            **patch_dict,
        )
        self.assertEqual(updated_site["owner"], self.user.pk)
        self.assertNotEqual(updated_site["owner"], 2)

    def test_can_partial_update_status(self):
        # Create a hypothetical site with 118 busy nodes, 10 idle nodes
        site = self.create_site()
        site["status"].update(
            dict(
                num_nodes=128,
                num_idle_nodes=10,
                num_busy_nodes=118,
                backfill_windows=[[8, 30], [2, 120]],
                queued_jobs=[
                    {
                        "queue": "foo",
                        "state": "queued",
                        "num_nodes": 64,
                        "score": 120,
                        "queued_time_min": 32,
                        "wall_time_min": 60,
                    },
                    {
                        "queue": "bar",
                        "state": "running",
                        "num_nodes": 54,
                        "score": 55,
                        "queued_time_min": 8,
                        "wall_time_min": 15,
                    },
                ],
            )
        )
        self.client.put_data(
            "site-detail", uri={"pk": site["pk"]}, check=status.HTTP_200_OK, **site
        )

        # Patch: 8 nodes taken; now 2 idle & 126 busy
        patch_dict = dict(
            status={
                "backfill_windows": [[2, 120]],
                "num_idle_nodes": 2,
                "num_busy_nodes": 126,
            }
        )
        target_site = site.copy()
        target_site["status"].update(**patch_dict["status"])

        updated_site = self.client.patch_data(
            "site-detail",
            uri={"pk": site["pk"]},
            check=status.HTTP_200_OK,
            **patch_dict,
        )

        # The patch was successful: updated_site is identical to expected
        # barring the "last_refresh" time stamp
        updated_site.pop("last_refresh")
        target_site.pop("last_refresh")
        self.assertDictEqual(updated_site, target_site)

    def test_deleting_site_removes_associated_backends(self):
        site = self.create_site()
        # Register a new app with one backend at this site
        app = self.client.post_data(
            "app-list",
            check=status.HTTP_201_CREATED,
            name="hello world",
            backends=[{"site": site["pk"], "class_name": "Demo.SayHello"}],
            parameters=["name", "N"],
        )
        backends = app["backends"]
        self.assertEqual(len(backends), 1)
        self.assertEqual(backends[0]["site"], site["pk"])

        # Now delete the site.  The app should remain, but with 0 backends.
        self.client.delete_data(
            "site-detail", uri={"pk": site["pk"]}, check=status.HTTP_204_NO_CONTENT
        )
        app = self.client.get_data(
            "app-detail", uri={"pk": app["pk"]}, check=status.HTTP_200_OK
        )
        self.assertEqual(len(app["backends"]), 0)
