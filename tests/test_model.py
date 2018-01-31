from tests.BalsamTestCase import BalsamTestCase
from balsam.service.models import BalsamJob, InvalidStateError

class BalsamJobDBTests(BalsamTestCase):
    '''Exercise direct manipulation of BalsamJob database'''

    def test_basic_addition(self):
        """ A job is added to the Balsam Job database.
        Updating state causes version number to change."""
        jobs = BalsamJob.objects.all()
        self.assertEqual(list(jobs), [])

        newjob = BalsamJob()
        version_old = newjob.version
        newjob.update_state('PREPROCESSED', 'using pre.py')
        version_new = newjob.version
        self.assertNotEqual(version_old, version_new)

    def test_first_save(self):
        '''The first and second Job DB saves don't cause problems'''
        newjob = BalsamJob()
        newjob.save()
        newjob.nodes = 10
        newjob.save()

    def test_concurrent_modify(self):
        '''Test optimistic lock mechanism'''
        from concurrency.exceptions import RecordModifiedError
        job1 = BalsamJob()
        job1.name = "Test1"
        job1.save()

        # simulating race condition...
        # another client grabs the same record
        job2 = BalsamJob.objects.get(pk=job1.pk)
        self.assertEquals(job1.pk, job2.pk)

        # job2 state is updated by the other client
        # then job1 tries to update description
        job2.update_state('RUNNING')
        job1.description = "Changed description"
        with self.assertRaises(RecordModifiedError):
            job1.save(update_fields=['description'])

    def test_get_set_parents(self):
        '''Can add and retreive parents from jobs'''
        job1, job2, job3 = (BalsamJob() for i in range(3))
        job1.name = "parent1"
        job1.save(update_fields=['name'])
        job2.name = "parent2"
        job2.save()
        job3.set_parents([job1, job2])

        job4 = BalsamJob()
        job4.set_parents([job2.pk, job3.pk])
        job4.save()

        self.assertFalse(job1.get_parents().exists())
        self.assertEqual(job2.get_parents_by_id(), [])

        job3.refresh_from_db()
        self.assertEqual(job3.get_parents_by_id(), [str(j) for j in (job1.pk,job2.pk)])

        p1, p2 = job4.get_parents()
        self.assertEqual(set([p1.pk, p2.pk]), set([job2.pk, job3.pk]))
