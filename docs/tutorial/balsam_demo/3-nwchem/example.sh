chmod +x *.py  # set exe permission
python apps.py # populate apps
balsam ls apps --verbose # check apps in DB

python populate.py
balsam ls # check jobs in DB

balsam submit-launch -n 4 -t 60 -A datascience -q debug-flat-quad --job-mode=mpi
