from fastapi.encoders import jsonable_encoder


class AuthError(Exception):
    pass


class RESTClient:
    def interactive_login(self):
        """Initiate interactive login flow"""
        raise NotImplementedError

    def refresh_auth(self):
        """
        Reload credentials if stored/not expired.
        Set appropriate Auth headers on HTTP session.
        """
        raise NotImplementedError

    def request(self, url, http_method, params=None, json=None, data=None):
        """
        Supports timeout retry, auto re-authentication, accepting DUPLICATE status
        Raises helfpul errors on 4**, 5**, TimeoutErrors, AuthErrors
        """
        raise NotImplementedError

    def get(self, url, **kwargs):
        """GET kwargs become URL query parameters (e.g. /?site=3)"""
        return self.request(url, "GET", params=kwargs)

    def post_form(self, url, **kwargs):
        return self.request(url, "POST", data=kwargs)

    def post(self, url, **kwargs):
        return self.request(url, "POST", json=jsonable_encoder(kwargs))

    def bulk_post(self, url, list_data):
        return self.request(url, "POST", json=jsonable_encoder(list_data))

    def put(self, url, **kwargs):
        return self.request(url, "PUT", json=jsonable_encoder(kwargs))

    def bulk_put(self, url, payload, **kwargs):
        return self.request(url, "PUT", json=jsonable_encoder(payload), params=kwargs)

    def patch(self, url, **kwargs):
        return self.request(url, "PATCH", json=jsonable_encoder(kwargs))

    def bulk_patch(self, url, list_data):
        return self.request(url, "PATCH", json=jsonable_encoder(list_data))

    def delete(self, url):
        return self.request(url, "DELETE")

    def bulk_delete(self, url, **kwargs):
        return self.request(url, "DELETE", params=kwargs)
