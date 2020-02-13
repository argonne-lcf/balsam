# flake8: noqa
"""LiveServer + Balsam Client API-driven tests"""

from django.test import LiveServerTestCase
from rest_framework import status


class BalsamClientFT(LiveServerTestCase):
    def setUp(self):
        return
        staging_server = os.environ.get("BALSAM_STAGING_SERVER")
        if staging_server:
            self.live_server_url = "http://" + staging_server
