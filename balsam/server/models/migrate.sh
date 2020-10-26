alembic revision --autogenerate -m "$*"
alembic upgrade head
