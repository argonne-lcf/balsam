from fastapi import status
from fastapi.encoders import jsonable_encoder


class BalsamTestClient:
    def __init__(self, client):
        self._client = client

    @property
    def headers(self):
        return self._client.headers

    def check_stat(self, expect_code, response):
        if expect_code is None:
            return
        if isinstance(expect_code, (list, tuple)):
            assert response.status_code in expect_code, response.text
        else:
            assert response.status_code == expect_code, response.text

    def get(self, url, check=status.HTTP_200_OK, **kwargs):
        """GET kwargs become URL query parameters (e.g. /?site=3)"""
        response = self._client.get(url, params=kwargs)
        self.check_stat(check, response)
        return response.json()

    def post_form(self, url, check=status.HTTP_201_CREATED, **kwargs):
        response = self._client.post(url, data=kwargs)
        self.check_stat(check, response)
        return response.json()

    def post(self, url, check=status.HTTP_201_CREATED, **kwargs):
        response = self._client.post(url, json=jsonable_encoder(kwargs))
        self.check_stat(check, response)
        return response.json()

    def bulk_post(self, url, list_data, check=status.HTTP_201_CREATED):
        response = self._client.post(url, json=jsonable_encoder(list_data))
        self.check_stat(check, response)
        return response.json()

    def put(self, url, check=status.HTTP_200_OK, **kwargs):
        response = self._client.put(url, json=jsonable_encoder(kwargs))
        self.check_stat(check, response)
        return response.json()

    def bulk_put(self, url, payload, check=status.HTTP_200_OK, **kwargs):
        response = self._client.put(url, json=payload, params=kwargs)
        self.check_stat(check, response)
        return response.json()

    def patch(self, url, check=status.HTTP_200_OK, **kwargs):
        response = self._client.patch(url, json=jsonable_encoder(kwargs))
        self.check_stat(check, response)
        return response.json()

    def bulk_patch(self, url, list_data, check=status.HTTP_200_OK):
        response = self._client.patch(url, json=jsonable_encoder(list_data))
        self.check_stat(check, response)
        return response.json()

    def delete(self, url, check=status.HTTP_204_NO_CONTENT):
        response = self._client.delete(url)
        self.check_stat(check, response)
        return response.json()

    def bulk_delete(self, url, check=status.HTTP_204_NO_CONTENT, **kwargs):
        response = self._client.delete(url, params=kwargs)
        self.check_stat(check, response)
        return response.json()


def create_site(
    client,
    hostname="baz",
    path="/foo",
    transfer_locations={},
    check=status.HTTP_201_CREATED,
):
    return client.post(
        "/sites/",
        hostname=hostname,
        path=path,
        transfer_locations=transfer_locations,
        check=check,
    )


def create_app(
    client,
    site_id,
    class_path="demo.SayHello",
    check=status.HTTP_201_CREATED,
    parameters=None,
    transfers=None,
):
    if parameters is None:
        parameters = {
            "name": {"required": True},
            "N": {"required": False, "default": 1},
        }
    if transfers is None:
        transfers = {
            "hello-input": {
                "required": False,
                "direction": "in",
                "local_path": "hello.yml",
                "description": "Input file for SayHello",
            }
        }
    return client.post(
        "/apps/",
        site_id=site_id,
        class_path=class_path,
        parameters=parameters,
        transfers=transfers,
        check=check,
    )
