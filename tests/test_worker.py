from tests.BalsamTestCase import BalsamTestCase
from balsam.models import BalsamJob
from django import db

class DumbTestCase(BalsamTestCase):
    def setUp(self):
        BalsamJob.objects.create(name="hello_testing!")
        assert db.connection.settings_dict['NAME'].endswith('test_db.sqlite3')

    def test_can_read(self):
        job = BalsamJob.objects.get(name__icontains="testing!")
        self.assertEqual(job.name, "hello_testing!")
