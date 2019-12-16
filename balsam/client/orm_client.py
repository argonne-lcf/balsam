import sys
from .client import ClientAPI

class DjangoORMClient(ClientAPI):
    _django_models = None

    @property
    def site(self):
        if self._site is None:
            self._site = self._django_models['Site'].get(pk=site_id)
        return self._site

    def _get_django_model(self, model_name):
        if self._django_models is None:
            from balsam.server import models
            self._django_models = models.MODELS
        model = self._django_models.get(model_name)
        if model is None:
            raise ValueError(f'Invalid model_name {model_name}')
        return model

    def test_connection(self, raises=False):
        """
        Returns True if client can reach Job DB.
        If raises=False, will supress db.OperationalError
        """
        from django import db
        db.connections.close_all()
        Job = self._django_models['Job']
        try:
            c = Job.objects.count()
        except db.OperationalError:
            if raises: raise
            return False
        else:
            return True

    def run_migrations(self):
        """
        Run Django migrations through an ORMClient
        """
        from django.core.management import call_command
        isatty = sys.stdout.isatty()
        call_command('migrate', interactive=isatty, verbosity=2)
        self.test_connection(raises=True)

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
        model = self._django_models.get(model_name)
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
