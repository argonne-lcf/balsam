import sys
import os
import django
import tempfile
import unittest

# import balsam
import subprocess


def set_permissions(top):
    os.chmod(top, 0o755)
    for root, subdirs, files in os.walk(top):
        for dir in (os.path.join(root, s) for s in subdirs):
            os.chmod(dir, 0o755)
        for file in (os.path.join(root, f) for f in files):
            os.chmod(file, 0o644)


def main():

    if '--temp' in ' '.join(sys.argv[1:]):
        test_dir = os.path.abspath(os.path.dirname(__file__))
        tempdir = tempfile.TemporaryDirectory(dir=test_dir, prefix="testdata_")
        test_directory = tempdir.name
        set_permissions(test_directory)

        os.environ['BALSAM_DB_PATH'] = os.path.expanduser(test_directory)
        p = subprocess.Popen(f'balsam init {test_directory}', shell=True)
        p.wait()
    else:
        db_path = os.environ.get('BALSAM_DB_PATH')
        if not db_path or 'test' not in db_path:
            print("Please set env BALSAM_DB_PATH to a balsam DB directory containing substring 'test'")
            sys.exit(1)

    os.environ['DJANGO_SETTINGS_MODULE'] = 'balsam.django_config.settings'
    django.setup()

    loader = unittest.defaultTestLoader
    if len(sys.argv) > 1:
        names = [n for n in sys.argv[1:] if '--' not in n]
    else:
        names = []

    if names:
        suite = loader.loadTestsFromNames(names)
    elif '--bench' not in ' '.join(sys.argv[1:]):
        suite = loader.discover('tests')
    else:
        suite = loader.discover('tests.benchmarks', pattern='bench*.py')
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    main()
