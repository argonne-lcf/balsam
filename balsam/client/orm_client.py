from balsam.server.conf import db_only
from .client import ClientAPI

class ORMClient(ClientAPI):
    def __init__(
        self,
        user,
        password,
        site_id,
        site_path,
        host,
        port,
        rel_db_path='balsamdb',
        db_name="balsam",
        engine='django.db.backends.postgresql',
        conn_max_age=60,
        db_options={
            'connect_timeout': 30,
            'client_encoding': 'UTF8',
            'default_transaction_isolation': 'read committed',
            'timezone': 'UTC',
        },
    ):
        """
        Initialize Django with a settings module for ORM usage only
        """
        db_only.set_database(
            user, password, host, port,
            db_name, engine, conn_max_age, db_options
        )
        from balsam.server import models
        self.DJANGO_MODELS = models.MODELS
        self.user = user
        self.password = password
        self.site_path = site_path
        self.site_id = site_id
        self.host = host
        self.port = port
        self.db_name = db_name
        self.engine = engine
        self.conn_max_age = conn_max_age
        self.db_options = db_options
        self.rel_db_path = rel_db_path

    def dict_config(self):
        d = {
            "user": self.user,
            "password": self.password,
            "site_id": self.site_id,
            "host": self.host,
            "port": self.port,
            "db_name": self.db_name,
            "engine": self.engine,
            "conn_max_age": self.conn_max_age,
            "db_options": self.db_options,
            "rel_db_path": self.rel_db_path,
        }
        return d

    @property
    def site(self):
        if self._site is None:
            self._site = self.DJANGO_MODELS['Site'].get(pk=site_id)
        return self._site

    def _get_django_model(self, model_name):
        model = self.DJANGO_MODELS.get(model_name)
        if model is None:
            raise ValueError(f'Invalid model_name {model_name}')
        return model

    def set_host_port(self, host, port):
        self.host = host
        self.port = port
        db_only.set_database(
            self.user, self.password,
            host, port,
            self.db_name, self.engine,
            self.conn_max_age, self.db_options
        )

    def list(
        self, model_name, field_names, 
        filters=None, excludes=None, order_by=None,
        limit=None, offset=None,
        count_only=False
    ):
        model = self._get_django_model(model_name)
        manager = model.site_manager(self._site) # TODO: site_manager limits queryset
        qs = manager.all().values(*field_names)

        if filters:
            qs = qs.filter(**filters)
        if excludes:
            qs = qs.exclude(**excludes)
        if order_by:
            qs = qs.order_by(*order_by)

        # Slice
        if limit and offset:
            qs = qs[offset:offset+limit]
        elif limit:
            qs = qs[:limit]
        elif offset:
            qs = qs[offset:]

        if count_only:
            return {'count': qs.count()}

        rows = list(qs)
        count = len(rows)
        return {'rows':rows, 'count':count}
    
    def create(self, model_name, instances):
        """
        Create from list of instances
        """
        model = self.DJANGO_MODELS.get(model_name)
        objs = [model(**instance_fields) for instance_fields in instances]

    def update(self, instances, fields):
        """
        Update the given fields of a set of instances
        """
        raise NotImplementedError
    
    def delete(self, instances):
        """
        Delete the instances
        """
        raise NotImplementedError
    
    def subscribe(self, model, filters=None, excludes=None):
        """
        Subscribe to Create/Update events on the query
        """
        raise NotImplementedError

    def acquire_jobs(self, context):
        """
        Let the server choose a set of jobs matching the context
        Returns jobs with lock acquired
        Starts daemon heartbeat thread to refresh lock
        """
        raise NotImplementedError

ClientAPI._class_registry['ORMClient'] = ORMClient
