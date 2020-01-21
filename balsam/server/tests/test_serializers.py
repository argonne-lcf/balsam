from rest_framework.test import APITestCase
from balsam.server.serializers import UserSerializer
from balsam.server.models import User, Site
from unittest import TestCase

class UserTests(APITestCase):

    def test_create_user(self):
        dat = dict(username='Bob')
        s = UserSerializer(data={'username': 'Bob'})
        valid = s.is_valid()
        self.assertTrue(valid)
        s.save()
        
        users = User.objects.all()
        bob = users.first()
        self.assertEqual(users.count(), 1)
        self.assertEqual(bob.username, 'Bob')

    def test_update_user(self):
        joe = User.objects.create_user(username='Joe', email='joe@aol.com')

        user_data = {'username': 'Joe', 'email': 'joe@gmail.com'}
        s = UserSerializer(joe, data=user_data)
        valid = s.is_valid()
        self.assertTrue(valid)
        s.save()

        joe_updated = User.objects.get(username='Joe')
        self.assertEqual(joe_updated.email, 'joe@gmail.com')

    def test_create_conflict(self):
        joe = User.objects.create_user(username='Joe', email='joe@aol.com')
        
        user_data = {'username': 'Joe', 'email': 'the_other_joe@aol.com'}
        s = UserSerializer(data=user_data)
        valid = s.is_valid()
        self.assertFalse(valid)
        self.assertIn('username', s.errors)

    def test_user_has_owned_sites(self):
        joe = User.objects.create_user(username='Joe', email='joe@aol.com')
        site_paths = {'/path1', '/path2'}
        for path in site_paths:
            Site.objects.create(
                owner=joe,
                hostname='foo',
                site_path=path,
            )

        s = UserSerializer(joe)
        self.assertIn('owned_sites', s.data)
        self.assertIn('authorized_sites', s.data)
        owned_sites = s.data['owned_sites']
        auth_sites = s.data['authorized_sites']
        self.assertSetEqual(set(owned_sites), set(auth_sites))

        qs = Site.objects.filter(pk__in=owned_sites)
        from_db = qs.values_list('site_path', flat=True)
        self.assertSetEqual(site_paths, set(from_db))
    
    def test_authorized_users_share_site(self):
        joe = User.objects.create_user(username='Joe', email='joe@aol.com')
        bob = User.objects.create_user(username='Bob')
        sites = (
            {'owner': joe, 'hostname': 'theta',  'site_path': '/path1'},
            {'owner': joe, 'hostname': 'theta',  'site_path': '/path2'},
            {'owner': bob, 'hostname': 'cooley', 'site_path': '/path3'},
        )
        for s in sites: Site.objects.create(**s)

        # No sharing: Joe owns 1 and 2.  Bob has 3.
        ser_joe = UserSerializer(joe)
        ser_bob = UserSerializer(bob)
        self.assertListEqual(ser_joe.data['authorized_sites'], [1, 2])
        self.assertListEqual(ser_bob.data['authorized_sites'], [3])

        # Site 2 is shared
        site2 = Site.objects.get(pk=2)
        site2.update(authorized_users=[joe, bob])

        # Now 2 appears for both users
        ser_joe = UserSerializer(joe)
        ser_bob = UserSerializer(bob)
        self.assertListEqual(ser_joe.data['authorized_sites'], [1, 2])
        self.assertListEqual(ser_bob.data['authorized_sites'], [2, 3])

class SiteTests(APITestCase):
    pass