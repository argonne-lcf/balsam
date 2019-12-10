from pathlib import Path
import yaml

class ClientAPI:
    
    INFO_FILENAME = Path('server-info')

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

    @staticmethod
    def from_yaml(site_path):
        fname = Path(site_path) / ClientAPI.INFO_FILENAME
        with open(fname) as fp:
            dat = yaml.safe_load(fp)
        dat['site_path'] = site_path
        cls_name = dat.pop('client_type')
        cls = ClientAPI._class_registry[cls_name]
        return cls(**dat)
