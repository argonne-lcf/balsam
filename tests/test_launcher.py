from tests.BalsamTestCase import BalsamTestCase
import subprocess

def cmdline(cmd):
    '''Return string output from a command line'''
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return p.communicate()[0].decode('utf-8')

class BalsamLauncherTests(BalsamTestCase):

    def test_whatever(self):
        stdout = cmdline('balsam ls')
        self.assertIn('No jobs found matching query', stdout)
