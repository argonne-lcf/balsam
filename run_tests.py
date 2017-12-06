import sys
import os
import django
import tempfile
import unittest



if __name__ == "__main__":
    tempdir = tempfile.TemporaryDirectory(dir=os.getcwd(), prefix="testdata_")
    
    os.environ['BALSAM_TEST_DIRECTORY'] = tempdir.name
    os.environ['BALSAM_TEST']='1'
    os.environ['DJANGO_SETTINGS_MODULE'] = 'argobalsam.settings'
    django.setup()

    loader = unittest.defaultTestLoader
    if len(sys.argv) > 1:
        names = sys.argv[1:]
        suite = loader.loadTestsFromNames(names)
    else:
        suite = loader.discover('tests')
    unittest.TextTestRunner(verbosity=2).run(suite)
