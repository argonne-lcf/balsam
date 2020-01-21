dropdb -U postgres balsam
createdb -U postgres balsam
rm models/migrations/????_*.py
./manage.py makemigrations
./manage.py migrate
python dev/bootstrap.py