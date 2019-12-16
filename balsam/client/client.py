from pathlib import Path
import yaml

class ClientAPI:
    
    INFO_FILENAME = Path('client.yml')

    _class_registry = {
    }

    def list(self, model, filters=None, excludes=None, order_by=None,
             limit=None, offset=None, fields=None):
        raise NotImplementedError
    
    def create(self, instances):
        raise NotImplementedError

    def update(self, instances, fields):
        raise NotImplementedError
    
    def delete(self, instances):
        raise NotImplementedError
    
    def subscribe(self, model, filters=None, excludes=None):
        raise NotImplementedError

    def acquire_jobs(self, launch_context):
        raise NotImplementedError
    
    def dict_config(self):
        raise NotImplementedError
    
    def dump_yaml(self):
        fpath = self.site_path / self.INFO_FILENAME
        dat = self.dict_config()
        dat["client_type"] = self.__class__.__name__
        with open(fpath, 'w') as fp:
            yaml.dump(self.dict_config(), fp)

    @classmethod
    def from_yaml(cls, site_path):
        fname = Path(site_path) / ClientAPI.INFO_FILENAME
        with open(fname) as fp:
            dat = yaml.safe_load(fp)
        dat['site_path'] = site_path

        cls_name = dat.pop('client_type', cls.__name__)
        if cls_name not in ClientAPI._class_registry:
            raise TypeError(f'Unregistered Client type: {cls_name}')

        ClientClass = ClientAPI._class_registry[cls_name]
        if not issubclass(ClientClass, cls):
            raise TypeError(f'{cls_name} is not a subclass of {cls.__name__}')

        return ClientClass(**dat)
    
    @classmethod
    def ensure_connection(cls, site_path):
        """
        Start the DB under `site_path` if it's not already running
        Concurrency-safe: uses DirLock for atomic check-and-start
        Returns client
        """
        site_path = Path(site_path)
        if not site_path.is_dir():
            raise FileNotFoundError(f'No site directory exists at {site_path}')

        with DirLock(site_path, 'db'):
            client = cls.from_yaml(site_path)
            if client.test_connection():
                banner("Connected to already running Balsam DB server!")
            else:
                client.establish_connection()
                client.test_connection(raises=True)
        return client
