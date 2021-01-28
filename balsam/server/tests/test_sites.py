from dateutil.parser import isoparse
from fastapi import status

from balsam.server import models

from .util import create_site


def test_create_site(auth_client):
    posted_site = create_site(
        auth_client,
        hostname="thetalogin3.alcf.anl.gov",
        path="/projects/myProject/balsam-site",
    )
    assert type(posted_site["id"]) == int
    site_list = auth_client.get("/sites/")["results"]
    assert isinstance(site_list, list)
    assert len(site_list) == 1
    assert site_list[0] == posted_site


def test_cannot_create_duplicate_site(auth_client):
    create_site(
        auth_client,
        hostname="theta",
        path="/projects/mysite1",
        check=status.HTTP_201_CREATED,
    )
    create_site(
        auth_client,
        hostname="theta",
        path="/projects/mysite1",
        check=status.HTTP_400_BAD_REQUEST,
    )


def test_detail_view(auth_client):
    created_site = create_site(auth_client)
    id = created_site["id"]
    retrieved_site = auth_client.get(f"sites/{id}")
    assert retrieved_site == created_site


def test_update_site_status(auth_client):
    created_site = create_site(auth_client)
    id = created_site["id"]
    created_time = isoparse(created_site["last_refresh"])
    created_site.update(dict(num_nodes=128))
    updated_site = auth_client.put(f"sites/{id}", **created_site)
    updated_time = isoparse(updated_site["last_refresh"])
    assert updated_time > created_time


def test_cannot_partial_update_owner(auth_client, db_session):
    created_site = create_site(auth_client)
    id = created_site["id"]
    owner_id = db_session.query(models.Site.owner_id).one()[0]
    other_owner_id = owner_id + 10
    auth_client.put(f"sites/{id}", owner_id=other_owner_id)
    assert db_session.query(models.Site.owner_id).one()[0] == owner_id


def test_deleting_site_removes_associated_apps(auth_client):
    site = create_site(auth_client)
    auth_client.post(
        "/apps/",
        site_id=site["id"],
        name="hello world",
        class_path="demo.SayHello",
        parameters={"name": {"required": True}, "N": {"required": False, "default": 1}},
    )
    assert len(auth_client.get("/apps/")["results"]) == 1
    auth_client.delete(f"/sites/{site['id']}", check=status.HTTP_204_NO_CONTENT)
    assert len(auth_client.get("/apps/")["results"]) == 0
