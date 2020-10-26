import requests
from balsam.api import Site, Manager
from balsam.client import BasicAuthRequestsClient

# Run this only once to register as a new user:
resp = requests.post(
    "http://localhost:8080/users/register",
    json={"username": "misha", "password": "foo123"},
)
print(resp.text)

# Login with the credentials used above:
client = BasicAuthRequestsClient(
    "http://localhost:8080", username="misha", password="foo123"
)

# Set up balsam.api to use client
Manager.set_client(client)
client.refresh_auth()  # Trade password for an access token

# Now you can freely use the client API
# See examples in balsam/api/test_api.py
if Site.objects.count() == 0:
    my_site = Site.objects.create(hostname="theta", path="/projects/foo")
print(Site.objects.all())
