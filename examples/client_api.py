import requests

from balsam.client import BasicAuthRequestsClient

# Run this only once to register as a new user:
resp = requests.post(
    "http://localhost:8000/users/register",
    json={"username": "misha2", "password": "foo123"},
)
print(resp.text)

# Login with the credentials used above:
client = BasicAuthRequestsClient("http://localhost:8000", username="misha2", password="foo123")
client.refresh_auth()  # Trade password for an access token

# Now you can freely use the client API
# See examples in tests/api/

Site = client.Site
if Site.objects.count() == 0:
    my_site = Site.objects.create(hostname="theta", path="/projects/foo")
print(Site.objects.all())
