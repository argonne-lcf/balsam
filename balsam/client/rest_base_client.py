from urllib.parse import urlencode


class AuthError(Exception):
    pass


class RESTClient:
    def __init__(self, api_root):
        self.api_root = api_root.rstrip("/")
        self.users = Resource(self, "users/")
        self.sites = Resource(self, "sites/")
        self.apps = AppResource(self, "apps/")
        self.batch_jobs = BatchJobResource(self, "batchjobs/")
        self.jobs = JobResource(self, "jobs/")
        self.events = Resource(self, "events/")
        self.sessions = SessionResource(self, "sessions/")

    def build_url(self, url, **query_params):
        result = self.api_root + "/" + url.lstrip("/")
        if query_params:
            query_seq = []
            for k, v in query_params.items():
                if isinstance(v, (list, tuple)):
                    query_seq.extend((k, item) for item in v)
                else:
                    query_seq.append((k, v))
            result += "?" + urlencode(query_seq)
        return result

    def interactive_login(self):
        """Initiate interactive login flow"""
        raise NotImplementedError

    def refresh_auth(self):
        """
        Reload credentials if stored/not expired.
        Set appropriate Auth headers on HTTP session.
        """
        raise NotImplementedError

    def request(self, absolute_url, http_method, payload=None):
        """
        Supports timeout retry, auto re-authentication, accepting DUPLICATE status
        Raises helfpul errors on 4**, 5**, TimeoutErrors, AuthErrors
        """
        raise NotImplementedError

    def extract_data(self, response):
        """Returns dict or list of Python primitive datatypes"""
        raise NotImplementedError


class Resource:
    def __init__(self, client, path):
        self.client = client
        self.collection_path = path

    def list(self, **query_params):
        url = self.client.build_url(self.collection_path, **query_params)
        response = self.client.request(url, "GET")
        return self.client.extract_data(response)

    def detail(self, uri, **query_params):
        url = self.client.build_url(f"{self.collection_path}{uri}", **query_params)
        response = self.client.request(url, "GET")
        return self.client.extract_data(response)

    def create(self, **payload):
        url = self.client.build_url(self.collection_path)
        response = self.client.request(url, "POST", payload=payload)
        return self.client.extract_data(response)

    def update(self, uri, payload, partial=False, **query_params):
        url = self.client.build_url(f"{self.collection_path}{uri}", **query_params)
        method = "PATCH" if partial else "PUT"
        response = self.client.request(url, method, payload=payload)
        return self.client.extract_data(response)

    def bulk_create(self, list_payload):
        url = self.client.build_url(self.collection_path)
        response = self.client.request(url, "POST", payload=list_payload)
        return self.client.extract_data(response)

    def bulk_update_query(self, patch, **query_params):
        """Apply the same patch_payload to every item selected by query_params"""
        url = self.client.build_url(self.collection_path, **query_params)
        response = self.client.request(url, "PUT", payload=patch)
        return self.client.extract_data(response)

    def bulk_update_patch(self, patch_list):
        url = self.client.build_url(self.collection_path)
        response = self.client.request(url, "PATCH", payload=patch_list)
        return self.client.extract_data(response)

    def destroy(self, uri, **query_params):
        url = self.client.build_url(f"{self.collection_path}{uri}", **query_params)
        self.client.request(url, "DELETE")
        return

    def bulk_destroy(self, **query_params):
        url = self.client.build_url(self.collection_path, **query_params)
        self.client.request(url, "DELETE")
        return


class JobResource(Resource):
    def history(self, uri):
        url = self.client.build_url(f"{self.collection_path}{uri}/events")
        response = self.client.request(url, "GET")
        return self.client.extract_data(response)


class AppResource(Resource):
    def merge(self, **payload):
        url = self.client.build_url(f"{self.collection_path}merge")
        response = self.client.request(url, "POST", payload=payload)
        return self.client.extract_data(response)


class BatchJobResource(Resource):
    def list_jobs(self, uri, **query_params):
        url = self.client.build_url(f"{self.collection_path}{uri}/jobs", **query_params)
        response = self.client.request(url, "GET")
        return self.client.extract_data(response)


class SessionResource(Resource):
    def acquire_jobs(self, uri, **payload):
        url = self.client.build_url(f"{self.collection_path}{uri}")
        response = self.client.request(url, "POST", payload=payload)
        return self.client.extract_data(response)
