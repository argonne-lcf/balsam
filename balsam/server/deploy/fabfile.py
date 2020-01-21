'''
Before using, be sure the SSH key to the server is loaded in agent:
ssh-add -K ~/.ssh/id_rsa_do

Then deploy to a particular host like this:
fab -i ~/.ssh/id_rsa_do deploy:host=misha@staging.balsam-flow.org
'''
import random
from fabric.contrib.files import append, exists
from fabric.api import cd, env, local, run

REPO_URL = 'https://github.com/masalim2/balsam.git'

def deploy():
    site_folder = f'/home/{env.user}/sites/{env.host}'
    run(f'mkdir -p {site_folder}')
    with cd(site_folder):
        old_pipfile = run('cat Pipfile.lock')
        _get_latest_source()
        new_pipfile = run('cat Pipfile.lock')
        if old_pipfile != new_pipfile:
            _update_virtualenv()
        else:
            print("No change to Pipfile.lock")
        _create_or_update_dotenv()
        _update_static_files()
        _wipe_out_and_update_database()
        _restart_gunicorn()

def _get_latest_source():
    if exists('.git'):
        run('git fetch')
    else:
        run(f'git clone {REPO_URL} .')
    current_commit = local("git log -n 1 --format=%H", capture=True)
    run(f'git reset --hard {current_commit}')

def _update_virtualenv():
    run(f'pipenv install')

def _create_or_update_dotenv():
    append('.env', 'DJANGO_DEBUG_FALSE=y')
    append('.env', f'SITENAME={env.host}')
    current_contents = run('cat .env')
    if 'DJANGO_SECRET_KEY' not in current_contents:
        new_secret = ''.join(random.SystemRandom().choices(
            'abcdefghijklmnopqrstuvwxyz0123456789', k=50
        ))
        append('.env', f'DJANGO_SECRET_KEY={new_secret}')

def _update_static_files():
    run('pipenv run python manage.py collectstatic --noinput')

def _wipe_out_and_update_database():
    run('./migrate.sh')

def _restart_gunicorn():
    run('sudo systemctl restart gunicorn-staging.balsam-flow.org')