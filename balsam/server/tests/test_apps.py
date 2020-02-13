from rest_framework import status
from balsam.server.models import AppBackend
from .clients import TestCase, TwoUserTestCase
from .mixins import SiteFactoryMixin, AppFactoryMixin


class AppTests(TestCase, SiteFactoryMixin, AppFactoryMixin):
    def test_can_create_app(self):
        site = self.create_site()
        app = self.create_app(sites=site, cls_names="DemoApp.hello")
        self.assertEqual(app["backends"][0]["class_name"], "DemoApp.hello")

    def test_created_app_in_list_view(self):
        site = self.create_site()
        app = self.create_app(sites=site, cls_names="DemoApp.hello")
        self.assertEqual(app["backends"][0]["site"], site["pk"])

        # Retrieve the app list; ensure the App shows up
        app_list = self.client.get_data("app-list", check=status.HTTP_200_OK)
        self.assertEqual(len(app_list), 1)
        self.assertDictEqual(app_list[0], app)

    def test_created_app_appears_on_site_detail(self):
        site = self.create_site()
        app = self.create_app(sites=site, cls_names="Foo.bar")
        app_retrieved = self.client.get_data("app-detail", uri={"pk": app["pk"]})
        self.assertDictEqual(app, app_retrieved)
        backend = app["backends"][0]
        self.assertEqual(backend["site"], site["pk"])
        self.assertEqual(backend["class_name"], "Foo.bar")

    def test_cannot_create_duplicate_name(self):
        site1 = self.create_site()
        site2 = self.create_site(hostname="otherhost")
        self.create_app(
            name="foo12",
            sites=site1,
            cls_names="Foo.bar",
            check=status.HTTP_201_CREATED,
        )
        self.create_app(
            name="foo12",
            sites=site2,
            cls_names="Foo.bar",
            check=status.HTTP_400_BAD_REQUEST,
        )

    def test_update_app_backends(self):
        site1 = self.create_site(hostname="a", path="/my/Project1")
        site2 = self.create_site(hostname="a", path="/my/Project2")
        site3 = self.create_site(hostname="b", path="/foo/bar")
        old_app = self.create_app(sites=site1, cls_names="Simulations.calcX")

        # Patch app to have 3 new backends
        backends_patch = [
            {"site": site1["pk"], "class_name": "renamed_simulation.calc"},
            {"site": site2["pk"], "class_name": "simulation.calc"},
            {"site": site3["pk"], "class_name": "simulation.calc"},
        ]
        app = self.client.patch_data(
            "app-detail",
            uri={"pk": old_app["pk"]},
            backends=backends_patch,
            check=status.HTTP_200_OK,
        )
        # The new backends match the intended patch (as far as site & class_name)
        new_backends = app.pop("backends")
        new_backends = [
            {"site": d["site"], "class_name": d["class_name"]} for d in new_backends
        ]
        self.assertListEqual(backends_patch, new_backends)
        # Otherwise, the app is unchanged
        old_app.pop("backends")
        self.assertDictEqual(old_app, app)

    def test_delete_app(self):
        site1 = self.create_site(hostname="a", path="/my/Project1")
        site2 = self.create_site(hostname="a", path="/my/Project2")
        self.create_app(
            name="foo_local",
            sites=site1,
            cls_names="Foo.bar",
            check=status.HTTP_201_CREATED,
        )
        app_shared = self.create_app(
            name="foo_dualsite",
            sites=[site1, site2],
            cls_names=["Foo.bar", "Foo.bar"],
            check=status.HTTP_201_CREATED,
        )
        # Peek into DB: there are only 2 backends
        self.assertEqual(AppBackend.objects.count(), 2)
        # Now the dual-backend app is deleted, leaving only the first backend
        self.client.delete_data(
            "app-detail", uri={"pk": app_shared["pk"]}, check=status.HTTP_204_NO_CONTENT
        )
        self.assertEqual(AppBackend.objects.count(), 1)
        sites = self.client.get_data("site-list")
        sites = {s["pk"]: s for s in sites}
        self.assertEqual(sites[site1["pk"]]["apps"], ["Foo.bar"])
        self.assertEqual(sites[site2["pk"]]["apps"], [])

    def test_app_merge(self):
        site1 = self.create_site(hostname="theta", path="/my/Project1")
        site2 = self.create_site(hostname="cooley", path="/my/Project2")
        app1 = self.create_app(
            name="foo_theta",
            sites=site1,
            cls_names="Foo.bar",
            check=status.HTTP_201_CREATED,
        )
        app2 = self.create_app(
            name="foo_cooley",
            sites=site2,
            cls_names="Foo.bar",
            check=status.HTTP_201_CREATED,
        )
        app3 = self.client.post_data(
            "app-merge",
            name="foo_merged",
            existing_apps=[app1["pk"], app2["pk"]],
            check=status.HTTP_201_CREATED,
        )
        self.assertEqual(app1["parameters"], app3["parameters"])
        self.assertEqual(len(app3["backends"]), 2)


class AppSharingTests(TwoUserTestCase):
    def test_no_shared_app(self):
        """client2 cannot see client1's apps by default"""
        site = self.client1.post_data(
            "site-list", check=status.HTTP_201_CREATED, hostname="baz", path="/foo"
        )
        app = self.client1.post_data(
            "app-list",
            check=status.HTTP_201_CREATED,
            name="hello world",
            backends=[{"site": site["pk"], "class_name": "Demo.SayHello"}],
            parameters=["name", "N"],
        )
        client1_apps = self.client1.get_data("app-list", check=status.HTTP_200_OK)
        self.assertEqual(len(client1_apps), 1)
        self.assertDictEqual(app, client1_apps[0])
        client2_apps = self.client2.get_data("app-list", check=status.HTTP_200_OK)
        self.assertEqual(len(client2_apps), 0)

    def test_shared_app(self):
        """If client1 shares his app with client2, then client2 can see it"""
        site = self.client1.post_data(
            "site-list", check=status.HTTP_201_CREATED, hostname="baz", path="/foo"
        )
        self.client1.post_data(
            "app-list",
            check=status.HTTP_201_CREATED,
            name="hello world",
            backends=[{"site": site["pk"], "class_name": "Demo.SayHello"}],
            parameters=["name", "N"],
            users=[self.user1.pk, self.user2.pk],
        )
        client1_apps = self.client1.get_data("app-list", check=status.HTTP_200_OK)
        client2_apps = self.client2.get_data("app-list", check=status.HTTP_200_OK)
        self.assertListEqual(client1_apps, client2_apps)

    def test_cannot_add_other_users_backend_to_app(self):
        site1 = self.client1.post_data(
            "site-list", check=status.HTTP_201_CREATED, hostname="baz", path="/foo"
        )
        site2 = self.client2.post_data(
            "site-list",
            check=status.HTTP_201_CREATED,
            hostname="baz",
            path="/projects/bar",
        )
        backend1 = {"site": site1["pk"], "class_name": "Demo.SayHello"}
        backend2 = {"site": site2["pk"], "class_name": "Demo.SayHello"}

        app1 = self.client1.post_data(
            "app-list",
            check=status.HTTP_201_CREATED,
            name="hello world",
            backends=[backend1],
            parameters=["name", "N"],
            users=[self.user1.pk, self.user2.pk],
        )
        self.client2.post_data(
            "app-list",
            check=status.HTTP_201_CREATED,
            name="hello world",
            backends=[backend2],
            parameters=["name", "N"],
            users=[self.user1.pk, self.user2.pk],
        )

        # Client1 can see both apps
        list1 = self.client1.get_data("app-list", check=status.HTTP_200_OK)
        self.assertEqual(len(list1), 2)

        # But Client1 cannot add a backend that doesn't belong to them
        app1["backends"].append({"site": site2["pk"], "class_name": "Demo.SayHello"})
        self.client1.put_data(
            "app-detail",
            check=status.HTTP_400_BAD_REQUEST,
            uri={"pk": app1["pk"]},
            **app1,
        )
