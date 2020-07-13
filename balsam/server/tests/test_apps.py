from fastapi import status
from .util import create_site, create_app


def test_created_app_in_list_view(auth_client):
    site = create_site(auth_client)
    app = create_app(auth_client, site["id"])

    # Retrieve the app list; ensure the App shows up
    app_list = auth_client.get("/apps")
    assert len(app_list) == 1
    assert app_list[0] == app


def test_filter_apps_by_site(auth_client):
    site1 = create_site(auth_client, path="/site/1")
    site2 = create_site(auth_client, path="/site/2")
    create_app(auth_client, site1["id"], class_path="demo.SayHelloA")
    create_app(auth_client, site1["id"], class_path="demo.SayHelloB")
    create_app(auth_client, site2["id"], class_path="demo.SayHelloC")

    assert len(auth_client.get("/apps", site_id=site1["id"])) == 2
    assert len(auth_client.get("/apps", site_id=site2["id"])) == 1


def test_cannot_create_duplicate(auth_client):
    site1 = create_site(auth_client)
    site2 = create_site(auth_client, hostname="otherhost")
    create_app(auth_client, site_id=site1["id"], class_path="Foo.bar")
    create_app(auth_client, site_id=site2["id"], class_path="Foo.bar")
    create_app(
        auth_client,
        site_id=site1["id"],
        class_path="Foo.bar",
        check=status.HTTP_400_BAD_REQUEST,
    )


def test_cannot_update_duplicate(auth_client):
    site = create_site(auth_client)
    create_app(auth_client, site_id=site["id"], class_path="a.A")
    app2 = create_app(auth_client, site_id=site["id"], class_path="a.B")
    auth_client.put(f"apps/{app2['id']}", class_path="a.C")
    auth_client.put(
        f"apps/{app2['id']}", class_path="a.A", check=status.HTTP_400_BAD_REQUEST
    )
    assert auth_client.get(f"apps/{app2['id']}")["class_path"] == "a.C"


def test_delete_app(auth_client):
    site = create_site(auth_client)
    assert len(auth_client.get("/apps")) == 0
    create_app(auth_client, site_id=site["id"])
    assert len(auth_client.get("/apps")) == 1
    app2 = create_app(auth_client, site_id=site["id"], class_path="app2.app")
    assert len(auth_client.get("/apps")) == 2

    auth_client.delete(f"/apps/{app2['id']}")
    assert len(auth_client.get("/apps")) == 1


def test_no_shared_app(create_user_client):
    """client2 cannot see client1's apps by default"""
    client1, client2 = create_user_client(), create_user_client()
    site = create_site(client1)
    create_app(client1, site_id=site["id"])
    assert len(client1.get("/apps")) == 1
    assert len(client2.get("/apps")) == 0


def test_cannot_add_app_to_other_user_site(create_user_client):
    client1, client2 = create_user_client(), create_user_client()
    site = create_site(client1)
    create_app(client2, site_id=site["id"], check=status.HTTP_404_NOT_FOUND)
