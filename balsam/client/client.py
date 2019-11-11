from .orm_client import ORMClient
from .rest_client import RESTClient

class ClientAPI:

    _client_registry = {
        'ORMClient': ORMClient, 
        'RESTClient': RESTClient,
    }

    @staticmethod
    def from_dict(client_class_name, **kwargs):
        cls = ClientAPI._client_registry.get(client_class_name)
        if cls is None:
            valid = ClientAPI._client_registry.keys()
            raise ValueError(
                 f"Invalid client_class_name {client_class_name} (must be one of: {valid})"
            )
        return cls(**kwargs)

    def list(self, model, filters=None, excludes=None, order_by=None,
             limit=None, offset=None, fields=None):
        """
        Invoke List
        """
        raise NotImplementedError
    
    def create(self, instances):
        """
        Create from list of instances
        """
        raise NotImplementedError

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
